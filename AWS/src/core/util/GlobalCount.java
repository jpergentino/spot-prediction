package core.util;

import java.util.HashMap;
import java.util.Map;

public class GlobalCount {
	
	private static Map<String, Integer> counters = new HashMap<>();
	
	private static void ensurePresenceOfKey(String key) {
		if (!counters.containsKey(key)) {
			counters.put(key, 0);
		}
	}
	
	public static Integer getCount(String key) {
		return counters.get(key);
	}
	
	public static void addCount(String key) {
		ensurePresenceOfKey(key);
		counters.put(key, counters.get(key) + 1);
	}

	public static void reduceCount(String key) {
		ensurePresenceOfKey(key);
		counters.put(key, counters.get(key) - 1);
	}

	
}
