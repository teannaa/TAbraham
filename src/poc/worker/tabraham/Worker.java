package poc.worker.tabraham;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;


public class Worker<T> {
	private List<Step<T>> steps;
	private ExecutionMode executionMode;

	private Worker(List<Step<T>> steps, ExecutionMode executionMode) {
		this.steps = steps;
		this.executionMode = executionMode;
	}

	public static <T> WorkerBuilder<T> sequential(Class<T> dataClass) {
		return new WorkerBuilder<>(dataClass, ExecutionMode.SEQUENTIAL);
	}

	public static <T> WorkerBuilder<T> concurrent(Class<T> dataClass) {
		return new WorkerBuilder<>(dataClass, ExecutionMode.CONCURRENT);
	}

	public static <T> WorkerBuilder<T> async(Class<T> dataClass) {
		return new WorkerBuilder<>(dataClass, ExecutionMode.ASYNC);
	}

	public void run(List<T> dataList) {
		switch (executionMode) {
		case SEQUENTIAL:
			runSequential(dataList);
			break;
		case CONCURRENT:
			runConcurrently(dataList);
			break;
		case ASYNC:
			runAsync(dataList);
			break;
		}
	}

	private void runSequential(List<T> dataList) {
		for (T data : dataList) {
			for (Step<T> step : steps) {
				try {
					step.execute(data);
				} catch (Exception e) {
					if (!step.isOptional()) {
						System.err.println("Step failed: " + e.getMessage());
						rollback(data, step);
						return;
					}
				}
			}
		}
	}

	private void runConcurrently(List<T> dataList) {
		ExecutorService executor = Executors.newFixedThreadPool(Math.min(10, dataList.size()));
		for (T data : dataList) {
			executor.submit(() -> {
				for (Step<T> step : steps) {
					try {
						step.execute(data);
					} catch (Exception e) {
						if (!step.isOptional()) {
							System.err.println("Step failed: " + e.getMessage());
							rollback(data, step);
							return;
						}
					}
				}
			});
		}
		executor.shutdown();
	}

	private void runAsync(List<T> dataList) {
	   /* Flux.fromIterable(dataList)
	        .flatMap(data -> Flux.fromIterable(steps)
	            .flatMap(step -> Mono.fromCallable(() -> {
	                try {
	                    step.execute(data);
	                    return true; // Signal success
	                } catch (Exception e) {
	                    if (!step.isOptional()) {
	                        System.err.println("Step failed: " + e.getMessage());
	                        rollback(data, step);
	                        throw new RuntimeException(e); 
	                    }
	                    return false; 
	                }
	            }).onErrorResume(e -> {
	                System.err.println("Error occurred: " + e.getMessage());
	                return Mono.just(false); 
	            }).subscribeOn(Schedulers.parallel()), 1)
	        )
	        .subscribe(); // Start the processing*/
	}



	private void rollback(T data, Step<T> failedStep) {
		List<Step<T>> executedSteps = new ArrayList<>();
		for (Step<T> step : steps) {
			if (step == failedStep) {
				break;
			}
			executedSteps.add(step);
		}
		for (int i = executedSteps.size() - 1; i >= 0; i--) {
			try {
				executedSteps.get(i).rollback(data);
			} catch (Exception e) {
				System.err.println("Rollback failed: " + e.getMessage());
			}
		}
	}

	public static class WorkerBuilder<T> {
		private Class<T> dataClass;
		private List<Step<T>> steps;
		private ExecutionMode executionMode;
		private int numberOfThreads = 10;

		public WorkerBuilder(Class<T> dataClass, ExecutionMode executionMode) {
			this.dataClass = dataClass;
			this.steps = new ArrayList<>();
			this.executionMode = executionMode;
		}

		// Method to set the number of threads
		public WorkerBuilder<T> threads(int numberOfThreads) {
			this.setNumberOfThreads(numberOfThreads);
			return this;
		}

		public StepBuilder<T> step() {
			return new StepBuilder<>(this);
		}

		public Worker<T> build() {
			return new Worker<>(steps, executionMode);
		}

		public Class<T> getDataClass() {
			return dataClass;
		}

		public List<Step<T>> getSteps() {
			return steps;
		}

		public int getNumberOfThreads() {
			return numberOfThreads;
		}

		public void setNumberOfThreads(int numberOfThreads) {
			this.numberOfThreads = numberOfThreads;
		}
	}

	public static class StepBuilder<T> {
		private WorkerBuilder<T> workerBuilder;
		private Step<T> step;

		public StepBuilder(WorkerBuilder<T> workerBuilder) {
			this.workerBuilder = workerBuilder;
			this.step = new Step<>();
		}

		public StepBuilder<T> action(Action<T> action) {
			step.setAction(action);
			return this;
		}

		public StepBuilder<T> retries(int retries) {
			step.setRetries(retries);
			return this;
		}

		public StepBuilder<T> optional() {
			step.setOptional(true);
			return this;
		}

		public StepBuilder<T> rollback(Action<T> rollback) {
			step.setRollback(rollback);
			return this;
		}

		public WorkerBuilder<T> add() {
			workerBuilder.getSteps().add(step);
			return workerBuilder;
		}
	}
}

class Step<T> {
	private Action<T> action;
	private int retries = 0;
	private boolean optional = false;
	private Action<T> rollback;

	public Action<T> getAction() {
		return action;
	}

	public void setAction(Action<T> action) {
		this.action = action;
	}

	public int getRetries() {
		return retries;
	}

	public void setRetries(int retries) {
		this.retries = retries;
	}

	public boolean isOptional() {
		return optional;
	}

	public void setOptional(boolean optional) {
		this.optional = optional;
	}

	public Action<T> getRollback() {
		return rollback;
	}

	public void setRollback(Action<T> rollback) {
		this.rollback = rollback;
	}

	public void execute(T data) {
		action.execute(data);
	}

	public void rollback(T data) {
		if (rollback != null) {
			rollback.execute(data);
		}
	}
}

interface Action<T> {
	void execute(T data);
}

enum ExecutionMode {
	SEQUENTIAL, CONCURRENT, ASYNC
}
