/**
 * 
 */
package org.hh.nosql;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import junit.framework.TestCase;

/**
 * @author Administrator
 *
 */
public class RowKeyTest extends TestCase {

	private int count;
	
	private Map<Long, AtomicInteger> map = new ConcurrentHashMap<Long, AtomicInteger>();
	
	protected void setUp() {
		count = 10000000;
	}
	
	public void testRowKey() {
		for (int i = 0; i < count; i++) {
			Long rowKey = genRowKey();
			if (!map.containsKey(rowKey)) {
				map.put(rowKey, new  AtomicInteger(0));
			}
			
			AtomicInteger cc = map.get(rowKey);
			cc.incrementAndGet();
		}
		
		List<Long> keys = new ArrayList<Long>();
		for (Map.Entry<Long, AtomicInteger> entry
				: map.entrySet()) {
			keys.add(entry.getKey());
		}
		
		Collections.sort(keys);
		
		for (Long key
				: keys) {
			System.out.println(key + " : " + map.get(key));
		}
	}
	
	private long genRowKey() {
		String uuid = genUUID();
		
		long hashCode = uuid.hashCode();
		// TODO 此处改成2的倍数取模会使性能增加
		return Math.abs(hashCode % 100);
	}
	
	private String genUUID() {
		UUID uuid = UUID.randomUUID();
		return uuid.toString().replaceAll("-", "");
	}
}
