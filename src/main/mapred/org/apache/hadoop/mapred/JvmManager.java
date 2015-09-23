/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.mapred;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Vector;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.TaskTracker.TaskInProgress;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.server.tasktracker.JVMInfo;
import org.apache.hadoop.mapreduce.server.tasktracker.userlogs.JvmFinishedEvent;
import org.apache.hadoop.util.ProcessTree.Signal;
import org.apache.hadoop.util.Shell.ShellCommandExecutor;

class JvmManager
{
	public static final Log LOG = LogFactory
			.getLog("org.apache.hadoop.mapred.JvmManager");

	private TaskTracker tracker = null;
	private JvmManagerForType mapJvmManager;
	private JvmManagerForType reduceJvmManager;

	public JvmEnv constructJvmEnv(List<String> setup, Vector<String> vargs,
			File stdout, File stderr, long logSize, File workDir, JobConf conf)
	{
		return new JvmEnv(setup, vargs, stdout, stderr, logSize, workDir, conf);
	}

	public JvmManager(TaskTracker tracker)
	{
		this.tracker = tracker;
		mapJvmManager = new JvmManagerForType(tracker.getMaxCurrentMapTasks(),
				true, tracker);
		reduceJvmManager = new JvmManagerForType(
				tracker.getMaxCurrentReduceTasks(), false, tracker);
	}

	// called from unit tests
	JvmManagerForType getJvmManagerForType(TaskType type)
	{
		if (type.equals(TaskType.MAP))
		{
			return mapJvmManager;
		}
		else if (type.equals(TaskType.REDUCE))
		{
			return reduceJvmManager;
		}
		return null;
	}

	/*
	 * Saves pid of the given taskJvm
	 */
	void setPidToJvm(JVMId jvmId, String pid)
	{
		if (jvmId.isMapJVM())
		{
			mapJvmManager.jvmIdToPid.put(jvmId, pid);
		}
		else
		{
			reduceJvmManager.jvmIdToPid.put(jvmId, pid);
		}
	}

	/*
	 * Returns the pid of the task
	 */
	String getPid(TaskRunner t)
	{
		if (t != null && t.getTask() != null)
		{
			if (t.getTask().isMapTask())
			{
				JVMId id = mapJvmManager.runningTaskToJvm.get(t);
				if (id != null)
				{
					return mapJvmManager.jvmIdToPid.get(id);
				}
			}
			else
			{
				JVMId id = reduceJvmManager.runningTaskToJvm.get(t);
				if (id != null)
				{
					return reduceJvmManager.jvmIdToPid.get(id);
				}
			}
		}
		return null;
	}

	public void stop() throws IOException, InterruptedException
	{
		mapJvmManager.stop();
		reduceJvmManager.stop();
	}

	public boolean isJvmKnown(JVMId jvmId)
	{
		if (jvmId.isMapJVM())
		{
			return mapJvmManager.isJvmknown(jvmId);
		}
		else
		{
			return reduceJvmManager.isJvmknown(jvmId);
		}
	}

	public void recoverMemorySpace(TaskRunner t) throws IOException,
			InterruptedException
	{
		long size = mapJvmManager.useMemorySpaceSize()
				+ reduceJvmManager.useMemorySpaceSize();
		long requreSize = size + t.getTask().getTaskRequireMemorySize();
		long maxMemory = this.tracker.fixResourceLimited
				.getTotalTaskMaxUseMemoryMb();
		if (requreSize > maxMemory)
		{
			LOG.info("requreSize > this.tracker.totalMemMb : " + requreSize
					+ ":" + maxMemory + ":" + size);
			long recoverSize = (requreSize - maxMemory) + 1024;
			mapJvmManager.recoverJVMMemory(t.getTask().getJobID(), recoverSize);
			reduceJvmManager.recoverJVMMemory(t.getTask().getJobID(),
					recoverSize);
		}
	}

	public void launchJvm(TaskRunner t, JvmEnv env) throws IOException,
			InterruptedException
	{
		if (t.getTask().isMapTask())
		{
			if (!mapJvmManager.isCanReuse(t))
				recoverMemorySpace(t);
			mapJvmManager.reapJvm(t, env);
		}
		else
		{
			if (!reduceJvmManager.isCanReuse(t))
				recoverMemorySpace(t);
			reduceJvmManager.reapJvm(t, env);
		}
	}

	public boolean validateTipToJvm(TaskInProgress tip, JVMId jvmId)
	{
		if (jvmId.isMapJVM())
		{
			return mapJvmManager.validateTipToJvm(tip, jvmId);
		}
		else
		{
			return reduceJvmManager.validateTipToJvm(tip, jvmId);
		}
	}

	public TaskInProgress getTaskForJvm(JVMId jvmId) throws IOException
	{
		if (jvmId.isMapJVM())
		{
			return mapJvmManager.getTaskForJvm(jvmId);
		}
		else
		{
			return reduceJvmManager.getTaskForJvm(jvmId);
		}
	}

	public void taskFinished(TaskRunner tr)
	{
		if (tr.getTask().isMapTask())
		{
			mapJvmManager.taskFinished(tr);
		}
		else
		{
			reduceJvmManager.taskFinished(tr);
		}
	}

	public void taskKilled(TaskRunner tr) throws IOException,
			InterruptedException
	{
		if (tr.getTask().isMapTask())
		{
			mapJvmManager.taskKilled(tr);
		}
		else
		{
			reduceJvmManager.taskKilled(tr);
		}
	}

	public void killJvm(JVMId jvmId) throws IOException, InterruptedException
	{
		if (jvmId.isMap)
		{
			mapJvmManager.killJvm(jvmId);
		}
		else
		{
			reduceJvmManager.killJvm(jvmId);
		}
	}

	/**
	 * Adds the task's work dir to the cleanup queue of taskTracker for
	 * asynchronous deletion of work dir.
	 * 
	 * @param tracker
	 *            taskTracker
	 * @param task
	 *            the task whose work dir needs to be deleted
	 */
	static void deleteWorkDir(TaskTracker tracker, Task task)
	{
		String user = task.getUser();
		String jobid = task.getJobID().toString();
		String taskid = task.getTaskID().toString();
		String workDir = TaskTracker.getTaskWorkDir(user, jobid, taskid,
				task.isTaskCleanupTask());
		String userDir = TaskTracker.getUserDir(user);
		tracker.getCleanupThread().addToQueue(
				new TaskController.DeletionContext(tracker.getTaskController(),
						false, user, workDir.substring(userDir.length())));

	}

	static class JvmManagerForType
	{
		// Mapping from the JVM IDs to running Tasks
		Map<JVMId, TaskRunner> jvmToRunningTask = new HashMap<JVMId, TaskRunner>();
		// Mapping from the tasks to JVM IDs
		Map<TaskRunner, JVMId> runningTaskToJvm = new HashMap<TaskRunner, JVMId>();
		// Mapping from the JVM IDs to Reduce JVM processes
		Map<JVMId, JvmRunner> jvmIdToRunner = new HashMap<JVMId, JvmRunner>();
		// Mapping from the JVM IDs to process IDs
		Map<JVMId, String> jvmIdToPid = new HashMap<JVMId, String>();

		int maxJvms;
		boolean isMap;
		private final long sleeptimeBeforeSigkill;

		Random rand = new Random(System.currentTimeMillis());
		static final String DELAY_BEFORE_KILL_KEY = "mapred.tasktracker.tasks.sleeptime-before-sigkill";
		// number of milliseconds to wait between TERM and KILL.
		private static final long DEFAULT_SLEEPTIME_BEFORE_SIGKILL = 250;
		private TaskTracker tracker;

		public JvmManagerForType(int maxJvms, boolean isMap, TaskTracker tracker)
		{
			this.maxJvms = maxJvms;
			this.isMap = isMap;
			this.tracker = tracker;
			sleeptimeBeforeSigkill = tracker.getJobConf().getLong(
					DELAY_BEFORE_KILL_KEY, DEFAULT_SLEEPTIME_BEFORE_SIGKILL);
		}

		synchronized public void setRunningTaskForJvm(JVMId jvmId, TaskRunner t)
		{
			jvmToRunningTask.put(jvmId, t);
			runningTaskToJvm.put(t, jvmId);
			jvmIdToRunner.get(jvmId).setBusy(true);
		}

		synchronized public boolean validateTipToJvm(TaskInProgress tip,
				JVMId jvmId)
		{
			if (jvmId == null)
			{
				LOG.warn("Null jvmId. Cannot verify Jvm. validateTipToJvm returning false");
				return false;
			}
			TaskRunner taskRunner = jvmToRunningTask.get(jvmId);
			if (taskRunner == null)
			{
				return false; // JvmId not known.
			}
			TaskInProgress knownTip = taskRunner.getTaskInProgress();
			if (knownTip == tip)
			{ // Valid to compare the addresses ? (or equals)
				return true;
			}
			else
			{
				return false;
			}
		}

		synchronized public TaskInProgress getTaskForJvm(JVMId jvmId)
				throws IOException
		{
			if (jvmToRunningTask.containsKey(jvmId))
			{
				// Incase of JVM reuse, tasks are returned to previously
				// launched
				// JVM via this method. However when a new task is launched
				// the task being returned has to be initialized.
				TaskRunner taskRunner = jvmToRunningTask.get(jvmId);
				JvmRunner jvmRunner = jvmIdToRunner.get(jvmId);
				Task task = taskRunner.getTaskInProgress().getTask();

				jvmRunner.taskGiven(task);
				return taskRunner.getTaskInProgress();

			}
			return null;
		}

		synchronized public boolean isJvmknown(JVMId jvmId)
		{
			return jvmIdToRunner.containsKey(jvmId);
		}

		synchronized public void taskFinished(TaskRunner tr)
		{
			JVMId jvmId = runningTaskToJvm.remove(tr);
			if (jvmId != null)
			{
				jvmToRunningTask.remove(jvmId);
				JvmRunner jvmRunner;
				if ((jvmRunner = jvmIdToRunner.get(jvmId)) != null)
				{
					jvmRunner.taskRan();
				}
			}
		}

		synchronized public void taskKilled(TaskRunner tr) throws IOException,
				InterruptedException
		{
			JVMId jvmId = runningTaskToJvm.remove(tr);
			if (jvmId != null)
			{
				jvmToRunningTask.remove(jvmId);
				killJvm(jvmId);
			}
		}

		synchronized public void killJvm(JVMId jvmId) throws IOException,
				InterruptedException
		{
			JvmRunner jvmRunner;
			if ((jvmRunner = jvmIdToRunner.get(jvmId)) != null)
			{
				killJvmRunner(jvmRunner);
			}
		}

		synchronized public void stop() throws IOException,
				InterruptedException
		{
			// since the kill() method invoked later on would remove
			// an entry from the jvmIdToRunner map, we create a
			// copy of the values and iterate over it (if we don't
			// make a copy, we will encounter concurrentModification
			// exception
			List<JvmRunner> list = new ArrayList<JvmRunner>();
			list.addAll(jvmIdToRunner.values());
			for (JvmRunner jvm : list)
			{
				killJvmRunner(jvm);
			}
		}

		private synchronized void killJvmRunner(JvmRunner jvmRunner)
				throws IOException, InterruptedException
		{
			jvmRunner.kill();
			removeJvm(jvmRunner.jvmId);
		}

		synchronized private void removeJvm(JVMId jvmId)
		{
			jvmIdToRunner.remove(jvmId);
			jvmIdToPid.remove(jvmId);
		}

		// ================================增加==============================
		// 确定当前运行JVM占用内存大小
		synchronized private long useMemorySpaceSize()
		{
			long useMemorySpaceSize = 0;
			Iterator<Map.Entry<JVMId, JvmRunner>> jvmIter = jvmIdToRunner
					.entrySet().iterator();
			while (jvmIter.hasNext())
			{
				JvmRunner jvmRunner = jvmIter.next().getValue();
				JobID jId = jvmRunner.jvmId.getJobId();
				long tmpSize = tracker.getJobMemory(jId, this.isMap);
				if (tmpSize != -1)
				{
					useMemorySpaceSize += tmpSize;
				}
			}
			return useMemorySpaceSize;
		}

		// 回收JVM内存
		synchronized private long recoverJVMMemory(JobID jobId, long recoverSize)
				throws IOException, InterruptedException
		{
			LOG.info("recoverJVMMemory start : recoverSize size : "
					+ recoverSize);
			List<JvmRunner> runnerToKill = new ArrayList<JvmRunner>();
			long realRecoverSize = 0;
			Iterator<Map.Entry<JVMId, JvmRunner>> jvmIter = jvmIdToRunner
					.entrySet().iterator();
			while (jvmIter.hasNext())
			{
				JvmRunner jvmRunner = jvmIter.next().getValue();
				JobID jId = jvmRunner.jvmId.getJobId();

				if ((jId.equals(jobId) && jvmRunner.ranAll())
						|| (!jId.equals(jobId) && !jvmRunner.isBusy()))
				{
					long tmpSize = tracker.getJobMemory(jId, this.isMap);
					if (tmpSize != -1)
						realRecoverSize += tmpSize;
					runnerToKill.add(jvmRunner);

					if (realRecoverSize >= recoverSize)
						break;
				}
			}
			for (JvmRunner killJvmRunner : runnerToKill)
			{
				LOG.info("Killing JVM: " + killJvmRunner.jvmId);
				killJvmRunner(killJvmRunner);
			}
			return realRecoverSize;
		}

		private synchronized boolean isCanReuse(TaskRunner t)
		{
			JobID jobId = t.getTask().getJobID();
			Iterator<Map.Entry<JVMId, JvmRunner>> jvmIter = jvmIdToRunner
					.entrySet().iterator();
			while (jvmIter.hasNext())
			{
				JvmRunner jvmRunner = jvmIter.next().getValue();
				JobID jId = jvmRunner.jvmId.getJobId();

				if (jId.equals(jobId) && !jvmRunner.isBusy()
						&& !jvmRunner.ranAll())
				{
					LOG.info("Reuse JVM");
					return true;
				}
			}
			return false;
		}

		private synchronized void reapJvm(TaskRunner t, JvmEnv env)
				throws IOException, InterruptedException
		{
			if (t.getTaskInProgress().wasKilled())
			{
				// the task was killed in-flight
				// no need to do the rest of the operations
				return;
			}

			JvmRunner runnerToKill = null;
			boolean spawnNewJvm = false;
			JobID jobId = t.getTask().getJobID();
			// ====================================================

			int numJvmsSpawned = jvmIdToRunner.size();
			if (numJvmsSpawned < maxJvms)
				spawnNewJvm = true;

			// 首先考虑重用JVM
			Iterator<Map.Entry<JVMId, JvmRunner>> jvmIter = jvmIdToRunner
					.entrySet().iterator();
			while (jvmIter.hasNext())
			{
				JvmRunner jvmRunner = jvmIter.next().getValue();
				JobID jId = jvmRunner.jvmId.getJobId();

				// look for a free JVM for this job; if one exists then just
				// break
				if (jId.equals(jobId) && !jvmRunner.isBusy()
						&& !jvmRunner.ranAll())
				{
					setRunningTaskForJvm(jvmRunner.jvmId, t); // reserve the JVM
					LOG.info("No new JVM spawned for jobId/taskid: " + jobId
							+ "/" + t.getTask().getTaskID()
							+ ". Attempting to reuse: " + jvmRunner.jvmId);
					return;
				}
				if (!spawnNewJvm && numJvmsSpawned >= maxJvms)
				{
					if ((jId.equals(jobId) && jvmRunner.ranAll())
							|| (!jId.equals(jobId) && !jvmRunner.isBusy()))
					{
						runnerToKill = jvmRunner;
						spawnNewJvm = true;
					}
				}
			}

			if (spawnNewJvm)
			{
				if (runnerToKill != null)
				{
					LOG.info("Killing JVM: " + runnerToKill.jvmId);
					killJvmRunner(runnerToKill);
				}
				spawnNewJvm(jobId, env, t);
				return;
			}
			// *MUST* never reach this
			LOG.fatal("Inconsistent state!!! "
					+ "JVM Manager reached an unstable state "
					+ "while reaping a JVM for task: "
					+ t.getTask().getTaskID() + " " + getDetails()
					+ ". Aborting. ");
			System.exit(-1);
		}

		private String getDetails()
		{
			StringBuffer details = new StringBuffer();
			details.append("Number of active JVMs:").append(
					jvmIdToRunner.size());
			Iterator<JVMId> jvmIter = jvmIdToRunner.keySet().iterator();
			while (jvmIter.hasNext())
			{
				JVMId jvmId = jvmIter.next();
				details.append("\n  JVMId ")
						.append(jvmId.toString())
						.append(" #Tasks ran: ")
						.append(jvmIdToRunner.get(jvmId).numTasksRan)
						.append(" Currently busy? ")
						.append(jvmIdToRunner.get(jvmId).busy)
						.append(" Currently running: ")
						.append(jvmToRunningTask.get(jvmId).getTask()
								.getTaskID().toString());
			}
			return details.toString();
		}

		private void spawnNewJvm(JobID jobId, JvmEnv env, TaskRunner t)
		{
			JvmRunner jvmRunner = new JvmRunner(env, jobId, t.getTask());
			jvmIdToRunner.put(jvmRunner.jvmId, jvmRunner);
			// spawn the JVM in a new thread. Note that there will be very
			// little
			// extra overhead of launching the new thread for a new JVM since
			// most of the cost is involved in launching the process. Moreover,
			// since we are going to be using the JVM for running many tasks,
			// the thread launch cost becomes trivial when amortized over all
			// tasks. Doing it this way also keeps code simple.
			jvmRunner.setDaemon(true);
			jvmRunner.setName("JVM Runner " + jvmRunner.jvmId + " spawned.");
			setRunningTaskForJvm(jvmRunner.jvmId, t);
			LOG.info(jvmRunner.getName());
			jvmRunner.start();
		}

		synchronized private void updateOnJvmExit(JVMId jvmId, int exitCode)
		{
			removeJvm(jvmId);
			TaskRunner t = jvmToRunningTask.remove(jvmId);

			if (t != null)
			{
				runningTaskToJvm.remove(t);
				if (exitCode != 0)
				{
					t.setExitCode(exitCode);
				}
				t.signalDone();
			}
		}

		class JvmRunner extends Thread
		{
			JvmEnv env;
			volatile boolean killed = false;
			volatile int numTasksRan;
			final int numTasksToRun;
			JVMId jvmId;
			volatile boolean busy = true;
			private ShellCommandExecutor shexec; // shell terminal for running
													// the task
			private Task firstTask;

			private List<Task> tasksGiven = new ArrayList<Task>();

			void taskGiven(Task task)
			{
				tasksGiven.add(task);
			}

			public JvmRunner(JvmEnv env, JobID jobId, Task firstTask)
			{
				this.env = env;
				this.jvmId = new JVMId(jobId, isMap, rand.nextInt());
				int numTasksForJVM = env.conf.getNumTasksToExecutePerJvm();
				if (isMap)
				{// 对于MAP的配置
					this.numTasksToRun = env.conf.getInt(
							"mapred.job.map.reuse.jvm.num.tasks",
							numTasksForJVM);
					LOG.info("mapred.job.map.reuse.jvm.num.tasks: "
							+ this.numTasksToRun);
				}
				else
				{// 对于Reduce的配置
					this.numTasksToRun = env.conf.getInt(
							"mapred.job.reduce.reuse.jvm.num.tasks",
							numTasksForJVM);
					LOG.info("mapred.job.reduce.reuse.jvm.num.tasks: "
							+ this.numTasksToRun);
				}
				this.firstTask = firstTask;
				LOG.info("In JvmRunner constructed JVM ID: " + jvmId);
			}

			@Override
			public void run()
			{
				try
				{
					runChild(env);
				}
				catch (InterruptedException ie)
				{
					return;
				}
				catch (IOException e)
				{
					LOG.warn("Caught IOException in JVMRunner", e);
				}
				catch (Throwable e)
				{
					LOG.error(
							"Caught Throwable in JVMRunner. Aborting TaskTracker.",
							e);
					System.exit(1);
				}
				finally
				{
					jvmFinished();
				}
			}

			public void runChild(JvmEnv env) throws IOException,
					InterruptedException
			{
				int exitCode = 0;
				try
				{
					env.vargs.add(Integer.toString(jvmId.getId()));
					TaskRunner runner = jvmToRunningTask.get(jvmId);
					if (runner != null)
					{
						Task task = runner.getTask();
						// Launch the task controller to run task JVM
						String user = task.getUser();
						TaskAttemptID taskAttemptId = task.getTaskID();
						String taskAttemptIdStr = task.isTaskCleanupTask() ? (taskAttemptId
								.toString() + TaskTracker.TASK_CLEANUP_SUFFIX)
								: taskAttemptId.toString();
						exitCode = tracker.getTaskController().launchTask(user,
								jvmId.jobId.toString(), taskAttemptIdStr,
								env.setup, env.vargs, env.workDir,
								env.stdout.toString(), env.stderr.toString());
					}
				}
				finally
				{ // handle the exit code
					// although the process has exited before we get here,
					// make sure the entire process group has also been killed.
					kill();
					updateOnJvmExit(jvmId, exitCode);
					LOG.info("JVM : " + jvmId + " exited with exit code "
							+ exitCode + ". Number of tasks it ran: "
							+ numTasksRan);
					deleteWorkDir(tracker, firstTask);
				}
			}

			private class DelayedProcessKiller extends Thread
			{
				private final String user;
				private final int pid;
				private final long delay;
				private final Signal signal;

				DelayedProcessKiller(String user, int pid, long delay,
						Signal signal)
				{
					this.user = user;
					this.pid = pid;
					this.delay = delay;
					this.signal = signal;
					setName("Task killer for " + pid);
					setDaemon(false);
				}

				@Override
				public void run()
				{
					try
					{
						Thread.sleep(delay);
						tracker.getTaskController().signalTask(user, pid,
								signal);
					}
					catch (InterruptedException e)
					{
						return;
					}
					catch (IOException e)
					{
						LOG.warn("Exception when killing task " + pid, e);
					}
				}
			}

			synchronized void kill() throws IOException, InterruptedException
			{
				if (!killed)
				{
					TaskController controller = tracker.getTaskController();
					// Check inital context before issuing a kill to prevent
					// situations
					// where kill is issued before task is launched.
					String pidStr = jvmIdToPid.get(jvmId);
					if (pidStr != null)
					{
						String user = env.conf.getUser();
						int pid = Integer.parseInt(pidStr);
						// start a thread that will kill the process dead
						if (sleeptimeBeforeSigkill > 0)
						{
							new DelayedProcessKiller(user, pid,
									sleeptimeBeforeSigkill, Signal.KILL)
									.start();
							controller.signalTask(user, pid, Signal.TERM);
						}
						else
						{
							controller.signalTask(user, pid, Signal.KILL);
						}
					}
					else
					{
						LOG.info(String.format(
								"JVM Not killed %s but just removed",
								jvmId.toString()));
					}
					killed = true;
				}
			}

			// Post-JVM-exit logs processing. inform user log manager
			private void jvmFinished()
			{
				JvmFinishedEvent jfe = new JvmFinishedEvent(new JVMInfo(
						TaskLog.getAttemptDir(firstTask.getTaskID(),
								firstTask.isTaskCleanupTask()), tasksGiven));
				tracker.getUserLogManager().addLogEvent(jfe);
			}

			public void taskRan()
			{
				busy = false;
				numTasksRan++;
			}

			public boolean ranAll()
			{
				return (numTasksRan == numTasksToRun);
			}

			public void setBusy(boolean busy)
			{
				this.busy = busy;
			}

			public boolean isBusy()
			{
				return busy;
			}
		}
	}

	static class JvmEnv
	{ // Helper class
		List<String> vargs;
		List<String> setup;
		File stdout;
		File stderr;
		File workDir;
		long logSize;
		JobConf conf;
		Map<String, String> env;

		public JvmEnv(List<String> setup, Vector<String> vargs, File stdout,
				File stderr, long logSize, File workDir, JobConf conf)
		{
			this.setup = setup;
			this.vargs = vargs;
			this.stdout = stdout;
			this.stderr = stderr;
			this.workDir = workDir;
			this.conf = conf;
		}
	}
}
