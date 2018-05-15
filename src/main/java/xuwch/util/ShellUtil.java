package xuwch.util;

import java.io.*;

public class ShellUtil {
	
	public static int exec(String cmd, String dir,
			String stdoutFile, String stderrFile) throws Exception {
		Process proc = null;
		String fullCmd = cmd + " >" + stdoutFile + " 2> " + stderrFile;
		try {
			System.out.println("start run: " + fullCmd);
			proc = Runtime.getRuntime().exec(
					new String[]{"sh", "-c", fullCmd},
					null, new File(dir));
			return proc.waitFor();
		} finally {
			if (proc != null) {
				try{ proc.getInputStream().close(); } catch(Exception e){}
				try{ proc.getOutputStream().close(); } catch(Exception e){}
				try{ proc.getErrorStream().close(); } catch(Exception e){}
			}
		}
	}
	
	// for test
	public static void main(String[] args) throws Exception {
		System.out.println("will run: " + args[0]);
		ShellUtil.exec(args[0], args[1], args[2], args[3]);
	}
}
