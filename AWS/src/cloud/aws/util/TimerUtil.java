package cloud.aws.util;

public class TimerUtil {
	
	public static void sleep(int seconds) {
		try {
			Thread.sleep(seconds * 1000);
		} catch (InterruptedException e) {
			System.out.println("Error when trying to sleep "+ seconds +" seconds: "+ e.getMessage());
		} 
	}

}
