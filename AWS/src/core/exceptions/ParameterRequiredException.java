package core.exceptions;

public class ParameterRequiredException extends Exception {
	
	private static final long serialVersionUID = -2661434493946987267L;

	public ParameterRequiredException(String parameter) {
		super(parameter);
	}

}
