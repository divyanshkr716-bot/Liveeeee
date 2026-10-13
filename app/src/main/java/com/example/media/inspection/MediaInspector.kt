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

        try {
            // Step 1: Open DataSource with strict isolation between extractor and retriever
            if (uri.scheme == "file" || uri.scheme == null) {
                val filePath = uri.path ?: ""
                val file = File(filePath)
                if (!file.exists() || !file.canRead()) {
                    throw java.io.FileNotFoundException("Local video file not found or unreadable: $filePath")
                }
                extractor.setDataSource(file.absolutePath)
                retriever.setDataSource(file.absolutePath)
            } else if (uri.scheme == "content" || uri.scheme == "android.resource") {
                // Open separate PFDs so retriever seeks don't corrupt extractor position
                pfdExtractor = try { context.contentResolver.openFileDescriptor(uri, "r") } catch (e: Exception) { null }
                pfdRetriever = try { context.contentResolver.openFileDescriptor(uri, "r") } catch (e: Exception) { null }

                if (pfdExtractor != null && pfdRetriever != null) {
                    extractor.setDataSource(pfdExtractor.fileDescriptor)
                    retriever.setDataSource(pfdRetriever.fileDescriptor)
                } else {
                    extractor.setDataSource(context, uri, null)
                    retriever.setDataSource(context, uri)
                }
            } else {
                val path = uri.path ?: uri.toString()
                extractor.setDataSource(path)
                retriever.setDataSource(path)
            }

            // Step 2: Extract container metadata via MediaMetadataRetriever
            val durStr = retriever.extractMetadata(MediaMetadataRetriever.METADATA_KEY_DURATION)
            if (!durStr.isNullOrEmpty()) {
                durationMs = durStr.toLongOrNull() ?: 0L
            }

            val wStr = retriever.extractMetadata(MediaMetadataRetriever.METADATA_KEY_VIDEO_WIDTH)
            val hStr = retriever.extractMetadata(MediaMetadataRetriever.METADATA_KEY_VIDEO_HEIGHT)
            if (!wStr.isNullOrEmpty()) width = wStr.toIntOrNull() ?: 0
            if (!hStr.isNullOrEmpty()) height = hStr.toIntOrNull() ?: 0

            // Account for video orientation rotation
            val rotStr = retriever.extractMetadata(MediaMetadataRetriever.METADATA_KEY_VIDEO_ROTATION)
            val rotation = rotStr?.toIntOrNull() ?: 0
            if (rotation == 90 || rotation == 270) {
                val temp = width
                width = height
                height = temp
            }

            val brStr = retriever.extractMetadata(MediaMetadataRetriever.METADATA_KEY_BITRATE)
            if (!brStr.isNullOrEmpty()) {
                videoBitrate = brStr.toLongOrNull() ?: 0L
            }

            val fpsStr = retriever.extractMetadata(MediaMetadataRetriever.METADATA_KEY_CAPTURE_FRAMERATE)
            if (!fpsStr.isNullOrEmpty()) {
                frameRate = fpsStr.toDoubleOrNull() ?: 0.0
            }

            val mimeStr = retriever.extractMetadata(MediaMetadataRetriever.METADATA_KEY_MIMETYPE)
            val containerFormat = if (!mimeStr.isNullOrEmpty()) mimeStr.substringAfterLast("/") else "mp4"

            // Step 3: Inspect tracks via MediaExtractor
            val numTracks = extractor.trackCount
            for (i in 0 until numTracks) {
                val format = extractor.getTrackFormat(i)
                val mime = format.getString(MediaFormat.KEY_MIME) ?: ""

                if (mime.startsWith("video/") && videoTrackIndex == -1) {
                    videoTrackIndex = i
                    videoMime = mime
                    if (width == 0 && format.containsKey(MediaFormat.KEY_WIDTH)) {
                        width = format.getInteger(MediaFormat.KEY_WIDTH)
                    }
                    if (height == 0 && format.containsKey(MediaFormat.KEY_HEIGHT)) {
                        height = format.getInteger(MediaFormat.KEY_HEIGHT)
                    }
                    if (frameRate == 0.0 && format.containsKey(MediaFormat.KEY_FRAME_RATE)) {
                        frameRate = format.getInteger(MediaFormat.KEY_FRAME_RATE).toDouble()
                    }
                    if (durationMs == 0L && format.containsKey(MediaFormat.KEY_DURATION)) {
                        durationMs = format.getLong(MediaFormat.KEY_DURATION) / 1000
                    }
                    if (videoBitrate == 0L && format.containsKey(MediaFormat.KEY_BIT_RATE)) {
                        videoBitrate = format.getInteger(MediaFormat.KEY_BIT_RATE).toLong()
                    }
                } else if (mime.startsWith("audio/") && audioTrackIndex == -1) {
                    audioTrackIndex = i
                    audioMime = mime
                    hasAudio = true
                    if (format.containsKey(MediaFormat.KEY_CHANNEL_COUNT)) {
                        audioChannels = format.getInteger(MediaFormat.KEY_CHANNEL_COUNT)
                    }
                    if (format.containsKey(MediaFormat.KEY_SAMPLE_RATE)) {
                        audioSampleRate = format.getInteger(MediaFormat.KEY_SAMPLE_RATE)
                    }
                    if (format.containsKey(MediaFormat.KEY_BIT_RATE)) {
                        audioBitrate = format.getInteger(MediaFormat.KEY_BIT_RATE).toLong()
                    }
                }
            }

            // Step 4: Measure real keyframe interval and timestamp monotonicity
            if (videoTrackIndex != -1) {
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
