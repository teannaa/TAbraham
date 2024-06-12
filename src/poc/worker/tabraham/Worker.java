package poc.worker.tabraham;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

public class Worker<T> {
    private List<Step<T>> steps;
    private ExecutionMode executionMode;
    private int numberOfThreads;

    private Worker(List<Step<T>> steps, ExecutionMode executionMode, int numberOfThreads) {
        this.steps = steps;
        this.executionMode = executionMode;
        this.numberOfThreads = numberOfThreads;
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

    public Job runJob(List<T> dataList) {
        switch (executionMode) {
            case SEQUENTIAL:
                return runSequential(dataList);
            case CONCURRENT:
                return runConcurrently(dataList);
            case ASYNC:
                return runAsync(dataList);
            default:
                throw new IllegalArgumentException("Invalid execution mode");
        }
    }

    private Job runSequential(List<T> dataList) {
        List<Runnable> tasks = new ArrayList<>();
        for (T data : dataList) {
            tasks.add(() -> executeSteps(data));
        }
        return new Job(tasks);
    }

    private Job runConcurrently(List<T> dataList) {
        ExecutorService executor = Executors.newFixedThreadPool(numberOfThreads);
        List<Runnable> tasks = new ArrayList<>();
        for (T data : dataList) {
            tasks.add(() -> executeSteps(data));
        }
        for (Runnable task : tasks) {
            executor.submit(task);
        }
        executor.shutdown();
        return new Job(tasks);
    }

    private Job runAsync(List<T> dataList) {
        return new Job(
            (List<Runnable>) Flux.fromIterable(dataList)
                .flatMap((T data) -> Flux.fromIterable(steps)
                    .flatMap((Step<T> step) -> Mono.fromRunnable(() -> {
                        try {
                            step.execute(data);
                        } catch (Exception e) {
                            if (!step.isOptional()) {
                                System.err.println("Step failed: " + e.getMessage());
                                rollback(data, step);
                            }
                        }
                    }).subscribeOn(Schedulers.parallel()))
                ).then()
        );
    }


    

    private void executeSteps(T data) {
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

        public WorkerBuilder<T> threads(int numberOfThreads) {
            this.numberOfThreads = numberOfThreads;
            return this;
        }

        public StepBuilder<T> step() {
            return new StepBuilder<>(this);
        }

        public Worker<T> build() {
            return new Worker<>(steps, executionMode, numberOfThreads);
        }

        public Class<T> getDataClass() {
            return dataClass;
        }

        public List<Step<T>> getSteps() {
            return steps;
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

    public static class Step<T> {
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

    public interface Action<T> {
        void execute(T data);
    }

    public enum ExecutionMode {
        SEQUENTIAL, CONCURRENT, ASYNC
    }

    public static class Job {
        private List<Runnable> tasks;

        public Job(List<Runnable> tasks) {
            this.tasks = tasks;
        }

        public void complete() {
            for (Runnable task : tasks) {
                task.run();
            }
        }
    }
}
