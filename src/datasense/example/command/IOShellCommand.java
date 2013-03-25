package datasense.example.command;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.common.collect.Lists;

public abstract class IOShellCommand implements Runnable {
	public abstract String getName();

	public abstract String create(File inputFile, File outputFile);

	public abstract String get();
	
	public String createSummary(File statFile, File outputFile) {
		return createSummary(statFile, outputFile, getName());
	}
	
	public abstract String createSummary(File statFile, File outputFile, String key);
	
	@Override
	public void run() {
		String command = get();
		if (command != null)
			try {
				String outputLine = null;
				
				 ArrayList<String> bashCommand = Lists.newArrayList(new String[] { "bash", "-c", command });
				ProcessBuilder pb = new ProcessBuilder(bashCommand);
				Process proc = pb.start();
				Logger.getLogger(this.getClass().getName()).log(Level.INFO,
						"execute command:" + bashCommand);
				
				BufferedReader inputReader = new BufferedReader(new InputStreamReader(
						proc.getInputStream()));
				
				while ((outputLine = inputReader.readLine()) != null)
					System.out.println(outputLine);
					
				inputReader.close();
				
				BufferedReader errorReader = new BufferedReader(new InputStreamReader(
						proc.getErrorStream()));
				while ((outputLine = errorReader.readLine()) != null)
					System.err.println(outputLine);
				errorReader.close();
				
				if (proc.waitFor() != 0) {
					throw new RuntimeException(getName()
							+ " process terminated with errors: "
							+ proc.getErrorStream().toString());
				}

			} catch (Exception e) {
				// TODO Auto-generated catch block
				Logger.getLogger(this.getClass().getName()).log(Level.SEVERE,
						"fail to execute the command:" + command, e);
				throw new RuntimeException(e);
			}
	}

	@Override
	public String toString() {
		return create(new File("/path/to/input/file"), new File(
				"/path/to/output/file"));
	}
	
}
