package org.apache.hadoop.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapred.TaskTrackerStatus;

public class FixResourceLimited extends Configured
{
	protected long totalTaskMaxUseMemoryMb;
	protected int totalTaskMaxUseProcNum;
	protected long currentOccupyMemMb = 0;
	protected int currentOccupyProcNum = 0;
	
	public FixResourceLimited(Configuration conf)
	{
		super(conf);
		this.totalTaskMaxUseMemoryMb = conf.getLong(
				"total.task.max.use.memory.mb", TaskTrackerStatus.UNAVAILABLE);
		this.totalTaskMaxUseProcNum = conf.getInt(
				"total.task.max.use.proc.num", TaskTrackerStatus.UNAVAILABLE);
	}
	
	public void setCurrentOccupyMemMb(long currentOccupyMemMb)
	{
		this.currentOccupyMemMb = currentOccupyMemMb;
	}
	
	public void setCurrentOccupyProcNum(int currentOccupyProcNum)
	{
		this.currentOccupyProcNum = currentOccupyProcNum;
	}
	
	public int getCurrentOccupyProcNum()
	{
		return this.currentOccupyProcNum;
	}
	
	public long getCurrentOccupyMemMb()
	{
		return this.currentOccupyMemMb;
	}

	public long getTotalTaskMaxUseMemoryMb()
	{
		return this.totalTaskMaxUseMemoryMb;
	}

	public int getTotalTaskMaxUseProcNum()
	{
		return this.totalTaskMaxUseProcNum;
	}
	
	public void setTotalTaskMaxUseMemoryMb(long totalTaskMaxUseMemoryMb)
	{
		this.totalTaskMaxUseMemoryMb = totalTaskMaxUseMemoryMb;
	}
	
	public void setTotalTaskMaxUseProcNum(int totalTaskMaxUseProcNum)
	{
		this.totalTaskMaxUseProcNum = totalTaskMaxUseProcNum;
	}
}
