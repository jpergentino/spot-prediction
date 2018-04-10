package core.util;

import java.nio.file.Paths;

import javax.sound.sampled.AudioInputStream;
import javax.sound.sampled.AudioSystem;
import javax.sound.sampled.Clip;

public class Sound {
	
	public static void playSiren() {
		
		Runnable r = new Runnable() {
			@Override
			public void run() {
				try {
					AudioInputStream audioIn = AudioSystem.getAudioInputStream(Paths.get("end.wav").toFile());
					Clip clip = AudioSystem.getClip();
					clip.open(audioIn);
					clip.start();
					Thread.sleep(8000);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		};
		
		new Thread(r).start();
	}
	
	public static void main(String[] args) {
		playSiren();
	}
}