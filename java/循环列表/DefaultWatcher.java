package com.ffcs.crmd.eagleeye.bootstrap.trace.watcher;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;

import com.ffcs.crmd.eagleeye.bootstrap.logging.ELogger;
import com.ffcs.crmd.eagleeye.bootstrap.logging.ELoggerFactory;
import com.ffcs.crmd.eagleeye.bootstrap.trace.Trace;
import com.ffcs.crmd.eagleeye.bootstrap.trace.sender.TraceSender;

/**
 * 轨迹Watcher
 * .
 * @author hehuang 20171001
 *
 */
public class DefaultWatcher implements Watcher {
	
	private static final ELogger logger = ELoggerFactory.getLogger(DefaultWatcher.class);

	private Slot[] slots;
	
	// 容量
	private final int capacity;
	
	// 当前指针位置
	private int position; 
	
	// 内部计时器
	private Timer timer;
	
	// 总的target数
	private int size;
	
	// 链溢出配置
	private int maxLength = -1;
	
	// max trace count
	private int maxTrace = -1;  
	
	private List<Interceptor> interceptors = new ArrayList<Interceptor>();
	
	private Map<Trace, Integer> coordinate = new ConcurrentHashMap<Trace, Integer>();

	private TraceSender overflowTraceSender;
	
	private TraceSender timeoutTraceSender;
	
	public DefaultWatcher(int capacity) {
		if (capacity <= 0) {
			throw new IllegalArgumentException("capacity must be positive!");
		}
		
		this.capacity = capacity;
		init();
	}
	
	private void init() {
		this.position = 0;
		this.size = 0;
		
		slots = new Slot[capacity]; // 初始化槽
		for (int i = 0; i < this.capacity; i++) {
			slots[i] = new Slot();
		}
		
		// 初始化指针Timer
		if (timer != null) {
			timer.cancel();
		}
		
		timer = new Timer("eagleeye-watch-timer", true);
		
		timer.schedule(new TimerTask() {
			
			@Override
			public void run() {
				position = (position + 1) % capacity;
				
				// 超时处理
				Slot slot = slots[position];
				List<Trace> traces = slot.clearTraces();
				for (Trace trace 
						: traces) {
					try {
						Trace cTrace = (Trace) trace.clone();
						
						timeoutTraceSender.sendTrace(cTrace);
					} catch (CloneNotSupportedException e) {
						if (logger.isWarnEnabled()) {
							logger.warn("超时轨迹发送失败: traceId -- {}", trace.getTraceId());
						}
					}
				}
			}
		}, 0L, 1000L);
	}
	
	public void addInterceptor(Interceptor interceptor) {
		this.interceptors.add(interceptor);
	}
	
	public void setMaxLength(int maxLength) {
		this.maxLength = maxLength;
	}
	
	public void setMaxTrace(int maxTrace) {
		this.maxTrace = maxTrace;
	}
	
	public void setOverflowTraceSender(TraceSender traceSender) {
		this.overflowTraceSender = traceSender;
	}
	
	public void setTimeoutTraceSender(TraceSender traceSender) {
		this.timeoutTraceSender = traceSender;
	}
	
	/**
	 * 定义Watcher的基本行为：
	 * 1. 更新最新操作时间：对未完成的监控更新最新操作时间
	 * 2. 新增操作时间：新增一个监控，添加操作时间
	 * 3. 任务完成：完成监控
	 * 
	 */
	public void addTarget(Trace trace) {
		// 判断是否溢出，如果存在链溢出提前发送
		if (this.maxLength != -1
				&& trace.getFinishedCount() >= this.maxLength) {
			try {
				// 通过复制方式避免并发
				Trace cTrace = (Trace) trace.clone();
				
				// 将已完成的链清理
				trace.clearRawTracks();
				overflowTraceSender.sendTrace(cTrace);
			} catch (CloneNotSupportedException e) {
				e.printStackTrace();
			}
			
		}
		
		int currentPos = this.position;
		int beforePos = currentPos - 1 > 0 ? currentPos - 1 : currentPos - 1 + capacity;
		
		coordinate.put(trace, beforePos);
		Slot slot = slots[beforePos];
		slot.addTarget(trace);
		size++;
	}
	
	public void refreshTarget(Trace target) {
		finishTarget(target);
		addTarget(target);
	}
	
	public void finishTarget(Trace target) {
		if (coordinate.containsKey(target)) {
			int pos = coordinate.remove(target);
			Slot slot = slots[pos];
			if (slot.removeTarget(target)) {
				size--;
			}
		}
	}
	
	
	
}

