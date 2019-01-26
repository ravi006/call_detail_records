package com.organization.ts.utils;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class RowHasher {

    private final static String salt = "DGE$5SGr@3VsHYUMas2323E4d57vfBfFSTRU@!DSH(*%FDSdfg13sgfsg";

    /**
     *
     * @param SquareId, To be hashed SquareId string.
     * @return md5 hash.
     */
    public static String md5Hash(String SquareId) {
        String md5 = null;
        if(null == SquareId)
            return null;

        SquareId = SquareId+salt;
        try {
            MessageDigest digest = MessageDigest.getInstance("MD5");//Create MessageDigest object for MD5
            digest.update(SquareId.getBytes(), 0, SquareId.length());//Update input string in message digest
            md5 = new BigInteger(1, digest.digest()).toString();
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        return md5;
    }

    public static void main(String[] args) {

        String message1 = "10001";
        String emptyMessage =  null;
        String message2 = "10002";
        System.out.println(message1 + " ::: MD5 hashed to >>>>>>> " + md5Hash(message1));
        System.out.println(emptyMessage +" ::: MD5 hashed to >>>>>>> " + md5Hash(null));
        System.out.println(message2 + " ::: MD5 hashed to >>>>>>> " + md5Hash(message2));
    }
}
