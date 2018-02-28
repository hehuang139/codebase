package com.ffcs.crmd.eagleeye.agent.sampling;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import com.ffcs.crmd.eagleeye.bootstrap.sampling.Sampler;
import com.ffcs.crmd.eagleeye.bootstrap.sampling.SamplingData;

/**
 * 组合采样器
 * .
 * @author hehuang 20171026
 *
 */
public class CompositePrioritySampler implements Sampler {
	
	private List<Sampler> samplers = new ArrayList<Sampler>();
	
	public CompositePrioritySampler(final List<Sampler> samplers) {
		this.samplers.addAll(samplers);
		
		sort();
	}
	
	/**
	 * 添加到最后
	 * .
	 * @param sampler
	 */
	public void composite(Sampler sampler) { // 添加到最后
		this.samplers.add(sampler);
	}
	
	/**
	 * 添加，并重新计算权重
	 * .
	 * @param sampler
	 */
	public void compositeOrderly(Sampler sampler) {
		this.samplers.add(sampler);
		sort();
	}
	

	@Override
	public boolean isSampling(SamplingData samplingData) {
		if (samplers.size() > 0) {
			for (Sampler sampler
					: samplers) {
				if (sampler.isMatch(samplingData)) {
					return sampler.isSampling(samplingData);
				}
			}
		}
		
		// 不应该走到这里
		return true;
	}

	@Override
	public boolean isMatch(SamplingData samplingData) {
		if (samplers.size() > 0) {
			for (Sampler sampler
					: samplers) {
				if (sampler.isMatch(samplingData)) {
					return true;
				}
			}
		}
		
		return false;
	}
	
	private void sort() {
		// 排序
		Collections.sort(this.samplers, new Comparator<Sampler> () {

			@Override
			public int compare(Sampler sampler1, Sampler sampler2) {
				if ((sampler1 instanceof Order)
						&& !(sampler2 instanceof Order)) {
					return 1;
				} else if ((sampler1 instanceof Order)
						&& (sampler2 instanceof Order)) {
					return ((Order) sampler1).getOrder() > ((Order) sampler2).getOrder() ? 1 : -1;
				} else {
					return -1;
				}
			}
		});
	}
	
	public String toString() {
		StringBuilder sb = new StringBuilder();
		
		sb.append("Composite: {")
			.append("\n");
		if (samplers != null
				&& samplers.size() > 0) {
			sb.append(samplers.get(0));
			
			for (int i = 1; i < samplers.size() - 1; i++) {
				Sampler sampler = samplers.get(i);
				sb.append(";\n")
				.append(sampler);
			}
		}
		sb.append("}");
		
		return sb.toString();
	}

}
