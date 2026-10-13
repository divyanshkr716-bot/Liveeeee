package com.example.media.inspection

import android.content.Context
import android.media.MediaExtractor
import android.media.MediaFormat
import android.media.MediaMetadataRetriever
import android.net.Uri
import android.os.ParcelFileDescriptor
import android.util.Log
import com.example.data.model.MediaMetadata
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import java.io.File
import java.io.FileDescriptor
import java.nio.ByteBuffer

class MediaInspector(private val context: Context) {

    private val TAG = "MediaInspector"

    suspend fun inspectMedia(uri: Uri, displayName: String, fileSize: Long): MediaMetadata = withContext(Dispatchers.IO) {
        val extractor = MediaExtractor()
        val retriever = MediaMetadataRetriever()
        var pfdExtractor: ParcelFileDescriptor? = null
        var pfdRetriever: ParcelFileDescriptor? = null

        var videoMime = "UNKNOWN"
        var audioMime = "UNKNOWN"
        var width = 0
        var height = 0
        var frameRate = 0.0
        var durationMs = 0L
        var videoBitrate = 0L
        var audioBitrate = 0L
        var audioChannels = 0
        var audioSampleRate = 0
        var hasAudio = false
        var videoTrackIndex = -1
        var audioTrackIndex = -1
        var measuredKeyframeIntervalSec: Double? = null
        var isContinuous = true
        extractor.selectTrack(videoTrackIndex)
                val syncTimesUs = mutableListOf<Long>()
                var prevSampleTimeUs = -1L
                val sampleBuffer = ByteBuffer.allocate(1024 * 128)
                var samplesChecked = 0

                while (samplesChecked < 240) {
                    val sampleSize = extractor.readSampleData(sampleBuffer, 0)
                    if (sampleSize < 0) break

                    val timeUs = extractor.sampleTime
                    val flags = extractor.sampleFlags

                    if (prevSampleTimeUs != -1L && timeUs < prevSampleTimeUs) {
                        isContinuous = false
                    }
                    prevSampleTimeUs = timeUs

                    if ((flags and MediaExtractor.SAMPLE_FLAG_SYNC) != 0) {
                        syncTimesUs.add(timeUs)
                        if (syncTimesUs.size >= 5) break
                    }

                    samplesChecked++
                    extractor.advance()
                }

                if (syncTimesUs.size >= 2) {
                    val intervalsUs = mutableListOf<Long>()
                    for (k in 1 until syncTimesUs.size) {
                        intervalsUs.add(syncTimesUs[k] - syncTimesUs[k - 1])
                    }
                    val avgIntervalUs = intervalsUs.average()
                    measuredKeyframeIntervalSec = avgIntervalUs / 1_000_000.0
                }

                if (frameRate <= 0.0 && durationMs > 0 && samplesChecked > 10) {
                    frameRate = 30.0
                }
            }

            if (videoTrackIndex == -1) {
                return@withContext MediaMetadata(
                    uriString = uri.toString(),
                    fileName = displayName,
                    containerFormat = containerFormat,
                    videoCodec = "None (Audio-Only or Unsupported)",
                    audioCodec = if (hasAudio) audioMime else "None",
                    width = 0,
                    height = 0,
                    durationMs = durationMs,
                    keyframeIntervalSec = 0.0,
                    isDirectStreamingCompatible = false,
                    compatibilitySummary = "No video track found in source file.",
                    fileSize = fileSize
                )
            }

            val isH264 = videoMime.equals(MediaFormat.MIMETYPE_VIDEO_AVC, ignoreCase = true)
            val isAac = audioMime.equals("audio/mp4a-latm", ignoreCase = true) || !hasAudio
            val gopSec = measuredKeyframeIntervalSec ?: 0.0
            val isKeyframeAcceptable = gopSec in 1.0..3.5

            val isDirectStreamingCompatible = isH264 && isAac && isContinuous && isKeyframeAcceptable

            val compatibilitySummary = buildString {
                if (isDirectStreamingCompatible) {
                    append("Source is compatible with YouTube Live (H.264/AAC, GOP ~${String.format("%.1f", gopSec)}s). Zero re-encoding passthrough enabled.")
                } else {
                    val reasons = mutableListOf<String>()
                    if (!isH264) reasons.add("Video codec is $videoMime (H.264 required)")
                    if (!isAac) reasons.add("Audio codec is $audioMime (AAC required)")
                    if (measuredKeyframeIntervalSec == null) {
                        reasons.add("Keyframe spacing could not be verified")
                    } else if (!isKeyframeAcceptable) {
                        reasons.add("Keyframe interval (${String.format("%.1f", gopSec)}s) is outside optimal 2.0s target")
                    }
                    if (!isContinuous) reasons.add("Timestamps have non-monotonic jumps")
                    append("Preparation required: ").append(reasons.joinToString("; "))
                }
            }

            MediaMetadata(
                uriString = uri.toString(),
                fileName = displayName,
                containerFormat = containerFormat,
                videoCodec = videoMime,
                audioCodec = if (hasAudio) audioMime else "None (Silent)",
                width = width,
                height = height,
                frameRate = frameRate,
                durationMs = durationMs,
                videoBitrateBps = videoBitrate,
                audioBitrateBps = audioBitrate,
                audioChannels = audioChannels,
                audioSampleRate = audioSampleRate,
                hasAudioTrack = hasAudio,
                keyframeIntervalSec = gopSec,
                isTimestampContinuous = isContinuous,
                isDirectStreamingCompatible = isDirectStreamingCompatible,
                compatibilitySummary = compatibilitySummary,
                fileSize = fileSize
            )

        } catch (se: SecurityException) {
            Log.e(TAG, "SecurityException inspecting media for $uri: ${se.message}")
            MediaMetadata(
                uriString = uri.toString(),
                fileName = displayName,
                videoCodec = "UNKNOWN (Permission Denied)",
                audioCodec = "UNKNOWN",
                compatibilitySummary = "Permission Denial: Android revoked access to this media URI. Please re-select the file using the Import button.",
                isDirectStreamingCompatible = false,
                fileSize = fileSize
            )
        } catch (e: Exception) {
            Log.e(TAG, "Error inspecting media: ${e.message}", e)
            val errClass = e::class.java.simpleName
            val errMsg = e.localizedMessage ?: e.message ?: "Unable to read media tracks"
            MediaMetadata(
                uriString = uri.toString(),
                fileName = displayName,
                videoCodec = "UNKNOWN ($errClass)",
                audioCodec = "UNKNOWN",
                compatibilitySummary = "Inspection error [$errClass]: $errMsg. Please verify the file is a valid video.",
                isDirectStreamingCompatible = false,
                fileSize = fileSize
            )
        } finally {
            try { extractor.release() } catch (e: Exception) {}
            try { retriever.release() } catch (e: Exception) {}
            try { pfdExtractor?.close() } catch (e: Exception) {}
            try { pfdRetriever?.close() } catch (e: Exception) {}
        }
    }
}
