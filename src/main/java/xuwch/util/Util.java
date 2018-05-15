package xuwch.util;

import java.util.*;
import java.io.*;

public class Util {

	public static Properties config;
	
	public static String mkString(ArrayList<String> list, String sep) {
		StringBuilder ss = new StringBuilder();
		boolean first = true;
		for (String s: list) {
			if (first) first = false;
			else ss.append(sep);
			ss.append(s);
		}
		return ss.toString();
	}
	
	public static<T> ArrayList<T> arrToList(T[] arr) {
		ArrayList<T> list = new ArrayList<T>();
		for(T e: arr) {
			list.add(e);
		}
		return list;
	}
	
	public static String getMapStrVal(HashMap<String, ArrayList<String>> map, String key) {
		if (map.containsKey(key)) {
			ArrayList<String> list = map.get(key);
			if (list.size() > 0) return list.get(0);
			else return null;
		} else return null;
	}
	
	public static String getMapStrValList(HashMap<String, ArrayList<String>> map, String key, String sep) {
		if (map.containsKey(key)) {
			ArrayList<String> list = map.get(key);
			if (list.size() > 0) return Util.mkString(list, sep);
			else return null;
		} else return null;
	}

	static public String readFileToString(String fileName) {  
        String encoding = "UTF-8";  
        File file = new File(fileName);
        if (!file.exists()) return null;
        Long filelength = file.length();  
        byte[] filecontent = new byte[filelength.intValue()];  
        try {  
            FileInputStream in = new FileInputStream(file);  
            in.read(filecontent);  
            in.close();  
        } catch (FileNotFoundException e) {  
            e.printStackTrace();  
        } catch (IOException e) {  
            e.printStackTrace();  
        }  
        try {  
            return new String(filecontent, encoding);  
        } catch (UnsupportedEncodingException e) {  
            System.err.println("The OS does not support " + encoding);  
            e.printStackTrace();  
            return null;  
        }  
    } 

	static public void writeTextFile(String fileName, String fileContent) throws IOException {
		File file = new File(fileName);
		FileWriter fw = new FileWriter(file);
		try {
			fw.write(fileContent);
		} catch (IOException e) {
			e.printStackTrace();
			throw e;
		} finally {
			if (fw != null) try { fw.close(); } catch (Exception e) {}
		}
	}

	static public byte[] readBinaryFile(String fileName) throws IOException {
		File file = new File(fileName);
		FileInputStream fis = new FileInputStream(file);
		try {
			long fileLength = file.length();
			byte[] buff = new byte[(int)fileLength];
			fis.read(buff);
			return buff;
		} catch (IOException e) {
			e.printStackTrace();
			throw e;
		} finally {
			if (fis != null) try { fis.close(); } catch (Exception e) {}
		}
	}

	static final Base64.Decoder base64Decoder = Base64.getDecoder();
	static final Base64.Encoder base64Encoder = Base64.getEncoder();

	static public byte[] base64Decode(String content) {
		return base64Decoder.decode(content);
	}

	static public String base64Encode(byte[] content) {
		return base64Encoder.encodeToString(content);
	}

	static public void writeBinaryFile(String fileName, byte[] fileContent) throws IOException {
		File file = new File(fileName);
		FileOutputStream fos = new FileOutputStream(file);
		try {
			fos.write(fileContent);
		} catch (IOException e) {
			e.printStackTrace();
			throw e;
		} finally {
			if (fos != null) try { fos.close(); } catch (Exception e) {}
		}
	}

	static Random rand = new Random(System.currentTimeMillis());
	public static String randomString(String prefix) {
		return prefix + String.valueOf(rand.nextLong());
	}

	public static int getStrIndexInArr(String key, String[] arr) {
		for (int i = 0; i < arr.length; ++i) {
			if (key.equals(arr[i])) return i;
		}
		return -1;
	}
	
	public static void main(String[] args) {

	}
}
