package com.pocketsignal

import android.content.Context
import android.security.keystore.KeyGenParameterSpec
import android.security.keystore.KeyProperties
import android.util.Base64
import java.security.KeyStore
import javax.crypto.Cipher
import javax.crypto.KeyGenerator
import javax.crypto.SecretKey
import javax.crypto.spec.GCMParameterSpec

internal class SecureApiKeyStore(context: Context) {
    private val preferences = context.getSharedPreferences(
        "secure_provider_credentials",
        Context.MODE_PRIVATE,
    )

    @Synchronized
    fun load(): String? {
        val ivValue = preferences.getString(IV_KEY, null)
        val encryptedValue = preferences.getString(VALUE_KEY, null)
        if (ivValue == null && encryptedValue == null) return null
        check(ivValue != null && encryptedValue != null) {
            "The saved API key is incomplete; remove it and enter it again"
        }
        val cipher = Cipher.getInstance(TRANSFORMATION)
        cipher.init(
            Cipher.DECRYPT_MODE,
            getKey(),
            GCMParameterSpec(TAG_LENGTH_BITS, Base64.decode(ivValue, Base64.NO_WRAP)),
        )
        return String(
            cipher.doFinal(Base64.decode(encryptedValue, Base64.NO_WRAP)),
            Charsets.UTF_8,
        )
    }

    @Synchronized
    fun save(apiKey: String) {
        require(apiKey.isNotBlank()) { "API key must not be blank" }
        val cipher = Cipher.getInstance(TRANSFORMATION)
        cipher.init(Cipher.ENCRYPT_MODE, getOrCreateKey())
        val encrypted = Base64.encodeToString(cipher.doFinal(apiKey.toByteArray()), Base64.NO_WRAP)
        val iv = Base64.encodeToString(cipher.iv, Base64.NO_WRAP)
        check(
            preferences.edit()
                .putString(IV_KEY, iv)
                .putString(VALUE_KEY, encrypted)
                .commit(),
        ) { "Could not securely save the API key" }
    }

    @Synchronized
    fun delete() {
        check(preferences.edit().remove(IV_KEY).remove(VALUE_KEY).commit()) {
            "Could not remove the saved API key"
        }
        val keyStore = keyStore()
        if (keyStore.containsAlias(KEY_ALIAS)) keyStore.deleteEntry(KEY_ALIAS)
    }

    private fun getKey(): SecretKey =
        keyStore().getKey(KEY_ALIAS, null) as? SecretKey
            ?: error("The saved API key cannot be decrypted; remove it and enter it again")

    private fun getOrCreateKey(): SecretKey {
        val keyStore = keyStore()
        if (keyStore.containsAlias(KEY_ALIAS)) return getKey()
        val generator = KeyGenerator.getInstance(
            KeyProperties.KEY_ALGORITHM_AES,
            ANDROID_KEY_STORE,
        )
        generator.init(
            KeyGenParameterSpec.Builder(
                KEY_ALIAS,
                KeyProperties.PURPOSE_ENCRYPT or KeyProperties.PURPOSE_DECRYPT,
            )
                .setBlockModes(KeyProperties.BLOCK_MODE_GCM)
                .setEncryptionPaddings(KeyProperties.ENCRYPTION_PADDING_NONE)
                .setRandomizedEncryptionRequired(true)
                .build(),
        )
        return generator.generateKey()
    }

    private fun keyStore(): KeyStore =
        KeyStore.getInstance(ANDROID_KEY_STORE).apply { load(null) }

    private companion object {
        const val ANDROID_KEY_STORE = "AndroidKeyStore"
        const val KEY_ALIAS = "pocket_signal_twelve_data_key"
        const val IV_KEY = "api_key_iv"
        const val VALUE_KEY = "api_key_ciphertext"
        const val TRANSFORMATION = "AES/GCM/NoPadding"
        const val TAG_LENGTH_BITS = 128
    }
}
