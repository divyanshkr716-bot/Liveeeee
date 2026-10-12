package com.example.media.inspection

import android.content.Context
import android.content.Intent
import android.net.Uri
import android.os.ParcelFileDescriptor
import android.util.Log
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import java.io.File
import java.io.FileNotFoundException
import java.io.FileOutputStream
import java.io.InputStream

object UriAccessHelper {
    private const val TAG = "UriAccessHelper"

    /**
     * Attempts to persist read permissions for a content URI.
     * Returns true if successfully persisted, false if provider does not support persistence.
     */
    fun takePersistablePermission(context: Context, uri: Uri): Boolean {
        if (uri.scheme != "content") return true
        return try {
            val flags = Intent.FLAG_GRANT_READ_URI_PERMISSION
            context.contentResolver.takePersistableUriPermission(uri, flags)
            true
        } catch (e: SecurityException) {
            Log.w(TAG, "Provider does not support persistable URI permissions for $uri: ${e.message}")
            false
        } catch (e: Exception) {
            Log.w(TAG, "Failed to take persistable URI permission for $uri: ${e.message}")
            false
        }
    }

    /**
     * Verifies that the URI can actually be opened for reading.
     */
    fun isUriAccessible(context: Context, uri: Uri): Boolean {
        return try {
            if (uri.scheme == "file" || uri.scheme == null) {
                val path = uri.path ?: return false
                val f = File(path)
                return f.exists() && f.canRead() && f.length() > 0
            }
            context.contentResolver.openFileDescriptor(uri, "r")?.use { pfd ->
                pfd.statSize > 0 || pfd.fileDescriptor.valid()
            } ?: false
        } catch (e: Exception) {
            false
        }
    }

    /**
     * Opens a ParcelFileDescriptor safely. Caller must close the returned PFD.
     */
    fun openFileDescriptor(context: Context, uri: Uri): ParcelFileDescriptor? {
        return try {
            if (uri.scheme == "file" || uri.scheme == null) {
                val file = File(uri.path ?: return null)
                ParcelFileDescriptor.open(file, ParcelFileDescriptor.MODE_READ_ONLY)
            } else {
                context.contentResolver.openFileDescriptor(uri, "r")
            }
        } catch (e: Exception) {
            Log.e(TAG, "Failed to open file descriptor for $uri: ${e.message}")
            null
        }
    }

    /**
     * Copies a content or external URI into app-controlled internal storage to guarantee access
     * across app restarts, foreground service lifecycles, and background tasks.
     * Includes disk space check, safe temp file write, and atomic rename.
     */
    suspend fun copyUriToInternalStorage(
        context: Context,
        uri: Uri,
        targetFileName: String,
        onProgress: ((Float) -> Unit)? = null
    ): Result<File> = withContext(Dispatchers.IO) {
        val sanitizedName = targetFileName.replace("[^a-zA-Z0-9._-]".toRegex(), "_")
        val importDir = File(context.filesDir, "imported_videos").apply { mkdirs() }
        val tempFile = File(importDir, "import_${System.currentTimeMillis()}_$sanitizedName.tmp")
        val finalFile = File(importDir, sanitizedName)

        var inputStream: InputStream? = null
        var outputStream: FileOutputStream? = null

        try {
            // Check file size and space before copying
            var expectedSize = 0L
            try {
                openFileDescriptor(context, uri)?.use { pfd ->
                    expectedSize = pfd.statSize
                }
            } catch (e: Exception) {
                // Size cannot be predetermined
            }

            val availableSpace = importDir.usableSpace
            if (expectedSize > 0 && availableSpace < expectedSize + 15_000_000L) {
                val neededMb = (expectedSize / (1024 * 1024))
                val freeMb = (availableSpace / (1024 * 1024))
                return@withContext Result.failure(
                    IllegalStateException("Insufficient internal storage to import video (requires ~${neededMb}MB, only ${freeMb}MB free).")
                )
            }

            inputStream = if (uri.scheme == "file" || uri.scheme == null) {
                val f = File(uri.path ?: "")
                if (!f.exists()) throw FileNotFoundException("Local file not found: ${uri.path}")
                f.inputStream()
            } else {
                context.contentResolver.openInputStream(uri)
                    ?: throw SecurityException("Permission denial or unable to open input stream for: $uri")
            }

            outputStream = FileOutputStream(tempFile)
            val buffer = ByteArray(1024 * 64)
            var bytesRead: Int
            var totalCopied = 0L

            while (inputStream.read(buffer).also { bytesRead = it } != -1) {
                outputStream.write(buffer, 0, bytesRead)
                totalCopied += bytesRead
                if (expectedSize > 0 && onProgress != null) {
                    onProgress((totalCopied.toFloat() / expectedSize).coerceIn(0f, 1f))
                }
            }
            outputStream.flush()
            outputStream.close()
            outputStream = null

            if (totalCopied == 0L) {
                tempFile.delete()
                return@withContext Result.failure(IllegalStateException("Selected file contains 0 bytes."))
            }

            if (finalFile.exists()) {
                finalFile.delete()
            }
            if (!tempFile.renameTo(finalFile)) {
                tempFile.copyTo(finalFile, overwrite = true)
                tempFile.delete()
            }

            Result.success(finalFile)
        } catch (se: SecurityException) {
            if (tempFile.exists()) tempFile.delete()
            Result.failure(SecurityException("Android permission denied for selected video: ${se.message}"))
        } catch (e: Exception) {
            if (tempFile.exists()) tempFile.delete()
            Result.failure(e)
        } finally {
            try { inputStream?.close() } catch (e: Exception) {}
            try { outputStream?.close() } catch (e: Exception) {}
        }
    }

    /**
     * Resolves a durable, accessible local URI for a newly selected media URI.
     * Always secures a copy in app-controlled storage to prevent background permission denial.
     */
    suspend fun resolveDurableVideoUri(
        context: Context,
        uri: Uri,
        displayName: String
    ): Result<Uri> {
        takePersistablePermission(context, uri)

        // If the file is already in app-internal storage, return as file URI directly
        if (uri.scheme == "file" && uri.path?.startsWith(context.filesDir.absolutePath) == true) {
            return Result.success(uri)
        }

        // Copy into app-controlled internal storage to guarantee access across background services and restarts
        val uniqueName = "${System.currentTimeMillis()}_$displayName"
        val copyResult = copyUriToInternalStorage(context, uri, uniqueName)
        return if (copyResult.isSuccess) {
            Result.success(Uri.fromFile(copyResult.getOrThrow()))
        } else {
            // If copy failed, check if original URI is directly accessible as fallback
            if (isUriAccessible(context, uri)) {
                Result.success(uri)
            } else {
                Result.failure(copyResult.exceptionOrNull() ?: Exception("Cannot access selected video."))
            }
        }
    }
}
