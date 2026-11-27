package tpf;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.util.HexFormat;
import javax.crypto.Cipher;
import javax.crypto.Mac;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;

public final class ChunkCrypto {

    public static final class Sealed {
        public final String id;
        public final int keyVersion;
        public final byte[] salt;
        public final byte[] nonce;
        public final byte[] ciphertext;

        public Sealed(String id, int keyVersion, byte[] salt, byte[] nonce, byte[] ciphertext) {
            this.id = id;
            this.keyVersion = keyVersion;
            this.salt = salt;
            this.nonce = nonce;
            this.ciphertext = ciphertext;
        }
    }

    private static final byte[] INFO = "tpf-chunk-v1".getBytes(StandardCharsets.UTF_8);
    private static final SecureRandom RNG = new SecureRandom();

    private final byte[] masterKey;
    private final int keyVersion;

    public ChunkCrypto(byte[] masterKey, int keyVersion) {
        if (masterKey.length != 32) throw new IllegalArgumentException("a master key deve ter 32 bytes");
        this.masterKey = masterKey.clone();
        this.keyVersion = keyVersion;
    }

    static byte[] hkdf(byte[] ikm, byte[] salt, byte[] info) throws GeneralSecurityException {
        Mac mac = Mac.getInstance("HmacSHA256");
        mac.init(new SecretKeySpec(salt, "HmacSHA256"));
        byte[] prk = mac.doFinal(ikm);
        mac.init(new SecretKeySpec(prk, "HmacSHA256"));
        mac.update(info);
        mac.update((byte) 0x01);
        return mac.doFinal();
    }

    static byte[] aad(int keyVersion, byte[] salt, byte[] nonce) {
        return ByteBuffer.allocate(6 + 4 + salt.length + nonce.length)
                .put("tpf-v1".getBytes(StandardCharsets.US_ASCII))
                .putInt(keyVersion).put(salt).put(nonce).array();
    }

    public static String sha256Hex(byte[] data) throws GeneralSecurityException {
        return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(data));
    }

    public Sealed seal(byte[] plaintext) throws GeneralSecurityException {
        byte[] salt = new byte[32];
        byte[] nonce = new byte[12];
        RNG.nextBytes(salt);
        RNG.nextBytes(nonce);
        byte[] dek = hkdf(masterKey, salt, INFO);
        Cipher c = Cipher.getInstance("AES/GCM/NoPadding");
        c.init(Cipher.ENCRYPT_MODE, new SecretKeySpec(dek, "AES"), new GCMParameterSpec(128, nonce));
        c.updateAAD(aad(keyVersion, salt, nonce));
        byte[] ct = c.doFinal(plaintext);
        return new Sealed(sha256Hex(ct), keyVersion, salt, nonce, ct);
    }

    public byte[] open(Sealed s) throws GeneralSecurityException {
        if (!sha256Hex(s.ciphertext).equals(s.id))
            throw new GeneralSecurityException("INTEGRIDADE: o hash difere de " + s.id);
        if (s.keyVersion != keyVersion)
            throw new GeneralSecurityException("versao de chave desconhecida: " + s.keyVersion);
        byte[] dek = hkdf(masterKey, s.salt, INFO);
        Cipher c = Cipher.getInstance("AES/GCM/NoPadding");
        c.init(Cipher.DECRYPT_MODE, new SecretKeySpec(dek, "AES"), new GCMParameterSpec(128, s.nonce));
        c.updateAAD(aad(s.keyVersion, s.salt, s.nonce));
        return c.doFinal(s.ciphertext);
    }
}