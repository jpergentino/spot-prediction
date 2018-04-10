package core.exceptions;

public class TooManyConnectionsException extends RuntimeException {
	
	private static final long serialVersionUID = 4073228954208528682L;
	private int maxPoolSize;
	private int actualSize;

	public TooManyConnectionsException(int maxPoolSize, int actualSize) {
		super("Too many connections created. Actual: "+ actualSize +" MAX: "+ maxPoolSize);
		this.maxPoolSize = maxPoolSize;
		this.actualSize = actualSize;
	}

	public int getMaxPoolSize() {
		return maxPoolSize;
	}

	public int getActualSize() {
		return actualSize;
	}

}
