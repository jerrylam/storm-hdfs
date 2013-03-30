package datasenses.example.command;

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
	
	/**
	 * return the name of the command
	 * 
	 * @return name of the command
	 */
	public abstract String getName();

	/**
	 * Create an executable command based on the inputFile and the outputFile
	 * 
	 * @param inputFile
	 * @param outputFile
	 * @return a command
	 */
	public abstract String create(File inputFile, File outputFile);

	/**
	 * retrieve the command
	 * 
	 * @return a command
	 */
	protected abstract String get();
	
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
