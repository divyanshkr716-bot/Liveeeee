package com.example.streaming.rtmp

import android.media.MediaExtractor
import android.media.MediaFormat
import android.util.Log
import kotlinx.coroutines.*
import java.io.ByteArrayOutputStream
import java.io.File
import java.nio.ByteBuffer

interface PacketSink {
    fun onAvcSequenceHeader(sps: ByteArray, pps: ByteArray)
    fun onAacSequenceHeader(config: ByteArray)
    fun onVideoPacket(packet: RtmpPacket.Video)
    fun onAudioPacket(packet: RtmpPacket.Audio)
    fun onLoopBoundary(newLoopIndex: Int, totalUptimeMs: Long)
}

class LoopPacketStreamer(
    private val preparedFile: File,
    private val sink: PacketSink
) {
    private var isStreaming = false
    private var streamingJob: Job? = null

    // Telemetry and verification counters (PRD Section 3 & 8)
    var encoderInvocationCount: Long = 0L
        private set
    var loopCount: Int = 0
        private set
    var totalVideoPacketsSent: Long = 0L
        private set
    var totalAudioPacketsSent: Long = 0L
        private set
    var sessionStartTimeMs: Long = 0L
        private set

    private val rebaser = TimestampRebaser()

    fun extractInitialHeaders(): Pair<Pair<ByteArray, ByteArray>?, ByteArray?> {
        val extractor = MediaExtractor()
        try {
            extractor.setDataSource(preparedFile.absolutePath)
            val trackCount = extractor.trackCount
            var videoTrackIndex = -1
            var audioTrackIndex = -1

            for (i in 0 until trackCount) {
                val format = extractor.getTrackFormat(i)
                val mime = format.getString(MediaFormat.KEY_MIME) ?: ""
                if (mime.startsWith("video/") && videoTrackIndex == -1) {
                    videoTrackIndex = i
                } else if (mime.startsWith("audio/") && audioTrackIndex == -1) {
                    audioTrackIndex = i
                }
            }

            val spsPps = if (videoTrackIndex != -1) {
                val videoFormat = extractor.getTrackFormat(videoTrackIndex)
                val raw = extractSpsAndPps(videoFormat, extractor, videoTrackIndex)
                if (raw.first != null && raw.second != null) {
                    Pair(raw.first!!, raw.second!!)
                } else null
            } else null

            val aacConfig = if (audioTrackIndex != -1) {
                val audioFormat = extractor.getTrackFormat(audioTrackIndex)
                extractAudioSpecificConfig(audioFormat)
            } else null

            return Pair(spsPps, aacConfig)
        } catch (e: Exception) {
            Log.e("LoopStreamer", "Failed to extract initial sequence headers", e)
            return Pair(null, null)
        } finally {
            try { extractor.release() } catch (e: Exception) {}
        }
    }

    fun start(scope: CoroutineScope) {
        if (isStreaming) return
        isStreaming = true
        sessionStartTimeMs = System.currentTimeMillis()
        loopCount = 0
        totalVideoPacketsSent = 0
        totalAudioPacketsSent = 0
        encoderInvocationCount = 0 // Verified 0: packet-level streaming bypasses video encoder
        rebaser.reset()

        streamingJob = scope.launch(Dispatchers.IO) {
            runLoopStreaming()
        }
    }

    fun stop() {
        isStreaming = false
        streamingJob?.cancel()
        streamingJob = null
    }

    private suspend fun runLoopStreaming() {
        val extractor = MediaExtractor()
        try {
            extractor.setDataSource(preparedFile.absolutePath)
            val trackCount = extractor.trackCount
            var videoTrackIndex = -1
            var audioTrackIndex = -1

            for (i in 0 until trackCount) {
                val format = extractor.getTrackFormat(i)
                val mime = format.getString(MediaFormat.KEY_MIME) ?: ""
                if (mime.startsWith("video/") && videoTrackIndex == -1) {
                    videoTrackIndex = i
                } else if (mime.startsWith("audio/") && audioTrackIndex == -1) {
                    audioTrackIndex = i
                }
            }

            if (videoTrackIndex == -1) {
                Log.e("LoopStreamer", "No video track found in prepared file.")
                return
            }

            // Extract SPS & PPS with support for split or combined csd-0
            val videoFormat = extractor.getTrackFormat(videoTrackIndex)
            val (sps, pps) = extractSpsAndPps(videoFormat, extractor, videoTrackIndex)
            if (sps != null && pps != null) {
                sink.onAvcSequenceHeader(sps, pps)
            } else {
                Log.w("LoopStreamer", "Could not extract SPS/PPS headers from video track.")
            }

            // Extract Audio Specific Config
            if (audioTrackIndex != -1) {
                val audioFormat = extractor.getTrackFormat(audioTrackIndex)
                val audioConfig = extractAudioSpecificConfig(audioFormat)
                if (audioConfig != null) {
                    sink.onAacSequenceHeader(audioConfig)
                }
            }

            // Select tracks
            extractor.selectTrack(videoTrackIndex)
            if (audioTrackIndex != -1) {
                extractor.selectTrack(audioTrackIndex)
            }

            val buffer = ByteBuffer.allocate(1024 * 512)
            var sessionStartRealtime = System.currentTimeMillis()
            var firstRebasedTsMs: Long = -1L
            var videoPacketsInCurrentLoop = 0L

            while (isStreaming && currentCoroutineContext().isActive) {
                val trackIndex = extractor.sampleTrackIndex
                if (trackIndex < 0) {
                    // Loop boundary reached! (PRD Section 8)
                    if (videoPacketsInCurrentLoop == 0L) {
                        Log.e("LoopStreamer", "Extractor reached EOF without reading any video packets in loop $loopCount. Aborting infinite loop.")
                        break
                    }
                    loopCount++
                    rebaser.onLoopRestart()
                    sink.onLoopBoundary(loopCount, System.currentTimeMillis() - sessionStartTimeMs)
                    videoPacketsInCurrentLoop = 0L
                    extractor.seekTo(0, MediaExtractor.SEEK_TO_CLOSEST_SYNC)
                    continue
                }

                val sampleSize = extractor.readSampleData(buffer, 0)
                if (sampleSize > 0) {
                    val rawSampleTimeUs = extractor.sampleTime
                    val flags = extractor.sampleFlags
                    val isKeyframe = (flags and MediaExtractor.SAMPLE_FLAG_SYNC) != 0

                    val rebasedTsUs = rebaser.rebaseTimestamp(rawSampleTimeUs)
                    val rebasedTsMs = rebasedTsUs / 1000L

                    // Real-time pacing: sleep if ahead of real-time clock to avoid network congestion
                    if (firstRebasedTsMs == -1L) {
                        firstRebasedTsMs = rebasedTsMs
                        sessionStartRealtime = System.currentTimeMillis()
                    } else {
                        val mediaElapsed = rebasedTsMs - firstRebasedTsMs
                        val realElapsed = System.currentTimeMillis() - sessionStartRealtime
                        val leadMs = mediaElapsed - realElapsed
                        if (leadMs > 15) {
                            delay(minOf(leadMs, 50))
                        }
                    }

                    val sampleData = ByteArray(sampleSize)
                    buffer.position(0)
                    buffer.get(sampleData)

                    if (trackIndex == videoTrackIndex) {
                        val naluFormatted = ensureLengthPrefixedNalu(sampleData)
                        val videoPacket = RtmpPacket.Video(
                            naluData = naluFormatted,
                            isKeyframe = isKeyframe,
                            timestampMs = rebasedTsMs
                        )
                        sink.onVideoPacket(videoPacket)
                        totalVideoPacketsSent++
                        videoPacketsInCurrentLoop++
                    } else if (trackIndex == audioTrackIndex) {
                        val rawAac = stripAdtsHeaderIfPresent(sampleData)
                        val audioPacket = RtmpPacket.Audio(
                            aacData = rawAac,
                            timestampMs = rebasedTsMs
                        )
                        sink.onAudioPacket(audioPacket)
                        totalAudioPacketsSent++
                    }
                }

                extractor.advance()
            }

        } catch (e: Exception) {
            if (e !is CancellationException) {
                Log.e("LoopStreamer", "Streaming loop encountered error: ${e.message}")
            }
        } finally {
            try {
                extractor.release()
            } catch (e: Exception) {
                // ignore
            }
        }
    }

    private fun extractSpsAndPps(
        videoFormat: MediaFormat,
        extractor: MediaExtractor,
        videoTrackIndex: Int
    ): Pair<ByteArray?, ByteArray?> {
        val csd0 = try { videoFormat.getByteBuffer("csd-0") } catch (e: Exception) { null }
        val csd1 = try { videoFormat.getByteBuffer("csd-1") } catch (e: Exception) { null }

        if (csd0 != null && csd1 != null) {
            val sps = extractNaluFromBuffer(csd0)
            val pps = extractNaluFromBuffer(csd1)
            return Pair(sps, pps)
        }

        if (csd0 != null) {
            // Check if csd0 contains both SPS and PPS delimited by 0x00000001
            val data = ByteArray(csd0.remaining())
            csd0.duplicate().get(data)
            val nalus = splitAnnexBNalus(data)
            var foundSps: ByteArray? = null
            var foundPps: ByteArray? = null
            for (nalu in nalus) {
                if (nalu.isNotEmpty()) {
                    val type = nalu[0].toInt() and 0x1F
                    if (type == 7 && foundSps == null) foundSps = nalu
                    if (type == 8 && foundPps == null) foundPps = nalu
                }
            }
            if (foundSps != null && foundPps != null) {
                return Pair(foundSps, foundPps)
            }
            if (foundSps != null) {
                return Pair(foundSps, csd1?.let { extractNaluFromBuffer(it) })
            }
        }

        return Pair(null, null)
    }

    private fun splitAnnexBNalus(data: ByteArray): List<ByteArray> {
        val list = mutableListOf<ByteArray>()
        var start = -1
        var i = 0
        while (i < data.size) {
            if (i <= data.size - 4 && data[i] == 0.toByte() && data[i + 1] == 0.toByte() && data[i + 2] == 0.toByte() && data[i + 3] == 1.toByte()) {
                if (start != -1 && i > start) {
                    list.add(data.copyOfRange(start, i))
                }
                start = i + 4
                i += 4
                continue
            } else if (i <= data.size - 3 && data[i] == 0.toByte() && data[i + 1] == 0.toByte() && data[i + 2] == 1.toByte()) {
                if (start != -1 && i > start) {
                    list.add(data.copyOfRange(start, i))
                }
                start = i + 3
                i += 3
                continue
            }
            i++
        }
        if (start != -1 && start < data.size) {
            list.add(data.copyOfRange(start, data.size))
        }
        return list
    }

    private fun extractAudioSpecificConfig(audioFormat: MediaFormat): ByteArray? {
        val csd0 = try { audioFormat.getByteBuffer("csd-0") } catch (e: Exception) { null }
        if (csd0 != null && csd0.remaining() > 0) {
            val config = ByteArray(csd0.remaining())
            csd0.duplicate().get(config)
            return config
        }

        // Generate standard 2-byte AAC-LC AudioSpecificConfig if not present
        val sampleRate = if (audioFormat.containsKey(MediaFormat.KEY_SAMPLE_RATE)) audioFormat.getInteger(MediaFormat.KEY_SAMPLE_RATE) else 44100
        val channels = if (audioFormat.containsKey(MediaFormat.KEY_CHANNEL_COUNT)) audioFormat.getInteger(MediaFormat.KEY_CHANNEL_COUNT) else 2

        val freqIdx = when (sampleRate) {
            96000 -> 0; 88200 -> 1; 64000 -> 2; 48000 -> 3; 44100 -> 4
            32000 -> 5; 24000 -> 6; 22050 -> 7; 16000 -> 8; 12000 -> 9
            11025 -> 10; 8000 -> 11; 7350 -> 12; else -> 4
        }
        val byte1 = ((2 shl 3) or (freqIdx shr 1)).toByte()
        val byte2 = (((freqIdx and 1) shl 7) or (channels shl 3)).toByte()
        return byteArrayOf(byte1, byte2)
    }

    private fun stripAdtsHeaderIfPresent(data: ByteArray): ByteArray {
        // ADTS syncword is 0xFFF (0xFF followed by 0xF0..0xFF)
        if (data.size >= 7 && (data[0].toInt() and 0xFF) == 0xFF && (data[1].toInt() and 0xF0) == 0xF0) {
            val hasProtection = (data[1].toInt() and 0x01) == 0
            val headerSize = if (hasProtection) 9 else 7
            if (data.size > headerSize) {
                return data.copyOfRange(headerSize, data.size)
            }
        }
        return data
    }

    private fun extractNaluFromBuffer(buffer: ByteBuffer): ByteArray {
        val dup = buffer.duplicate()
        // Strip annex-b 0x00000001 start code if present
        if (dup.remaining() >= 4 && dup.get(0) == 0.toByte() && dup.get(1) == 0.toByte() && dup.get(2) == 0.toByte() && dup.get(3) == 1.toByte()) {
            dup.position(4)
        } else if (dup.remaining() >= 3 && dup.get(0) == 0.toByte() && dup.get(1) == 0.toByte() && dup.get(2) == 1.toByte()) {
            dup.position(3)
        }
        val out = ByteArray(dup.remaining())
        dup.get(out)
        return out
    }

    private fun ensureLengthPrefixedNalu(sample: ByteArray): ByteArray {
        val startsWith4ByteAnnexB = sample.size >= 4 && sample[0] == 0.toByte() && sample[1] == 0.toByte() && sample[2] == 0.toByte() && sample[3] == 1.toByte()
        val startsWith3ByteAnnexB = sample.size >= 3 && sample[0] == 0.toByte() && sample[1] == 0.toByte() && sample[2] == 1.toByte()

        if (startsWith4ByteAnnexB || startsWith3ByteAnnexB) {
            val nalus = splitAnnexBNalus(sample)
            val bos = ByteArrayOutputStream(sample.size + nalus.size * 4)
            for (nalu in nalus) {
                if (nalu.isNotEmpty()) {
                    val len = nalu.size
                    bos.write((len shr 24) and 0xFF)
                    bos.write((len shr 16) and 0xFF)
                    bos.write((len shr 8) and 0xFF)
                    bos.write(len and 0xFF)
                    bos.write(nalu)
                }
            }
            return bos.toByteArray()
        }
        return sample
    }
}
