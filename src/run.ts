import { IExecutor } from './Executor';
import ITask from './Task';

export default async function run(executor: IExecutor, queue: AsyncIterable<ITask>, maxThreads = 0) {
    const threadsLimit = maxThreads > 0 ? maxThreads : Number.MAX_SAFE_INTEGER;
    const waitingQueue: WaitingTasksQueue = new Map<number, ILinkedList<ITask>>([]);
    const currentRunning = new Set<number>([]);

    const [resultPromise, resolveResult] = createResolvablePromise();

    executeInitial(queue, waitingQueue, currentRunning, executor, resolveResult, threadsLimit);

    return resultPromise;
}

function executeNext (
    queue: AsyncIterable<ITask>,
    waitingQueue: WaitingTasksQueue,
    currentRunning: Set<number>,
    executor: IExecutor,
    task: ITask,
    onResolve: () => void,
    threadsLimit: number
) {
    currentRunning.add(task.targetId);
    executor
        .executeTask(task)
        .then(async () => {
            currentRunning.delete(task.targetId);
            const followingTask = await takeNextTask(queue, waitingQueue, currentRunning, task);

            if (!followingTask && !currentRunning.size && !waitingQueue.size) {
                onResolve();
                return;
            }

            if (followingTask) {
                executeNext(queue, waitingQueue, currentRunning, executor, followingTask, onResolve, threadsLimit);
            }

            executeInitial(queue, waitingQueue, currentRunning, executor, onResolve, threadsLimit);

        });
}

async function executeInitial(
    queue: AsyncIterable<ITask>,
    waitingQueue: WaitingTasksQueue,
    currentRunning: Set<number>,
    executor: IExecutor,
    onResolve: () => void,
    threadsLimit: number
) {
    while (currentRunning.size < threadsLimit) {
        const newTask = await getNewTask(queue, waitingQueue, currentRunning);

        if (!newTask) {
            break;
        }

        executeNext(queue, waitingQueue, currentRunning, executor, newTask, onResolve, threadsLimit);
    }
}

async function takeNextTask (
    queue: AsyncIterable<ITask>,
    waitingTasks: WaitingTasksQueue,
    currentRunning: Set<number>,
    previous: ITask
) {
    const waiting = dequeueTask(waitingTasks, previous.targetId);

    if (waiting) {
        return waiting;
    }

    return await getNewTask(queue, waitingTasks, currentRunning);
}

async function getNewTask(
    queue: AsyncIterable<ITask>,
    waitingTasks: WaitingTasksQueue,
    currentRunning: Set<number>
): Promise<ITask | null> {
    const iterator = await queue[Symbol.asyncIterator]().next();
    if (iterator.done || !iterator.value) {
        return null;
    }

    const { value } = iterator;
    if (!currentRunning.has(value.targetId)) {
        return value;
    }

    enqueueTask(waitingTasks, value);

    return getNewTask(queue, waitingTasks, currentRunning);
}

// #region Linked list util
interface ILinkedList<TValue> {
    value: TValue;
    tail: ILinkedList<TValue> | null;
}

function pushTail<TValue>(list: ILinkedList<TValue>, value: TValue) {
    let last = list;

    while (last.tail) {
        last = last.tail;
    }

    last.tail = { value, tail: null };
}

// #endregion

// #region Queue util
type WaitingTasksQueue = Map<number, ILinkedList<ITask>>;

function enqueueTask (queue: WaitingTasksQueue, value: ITask) {
    if (!queue.has(value.targetId)) {
        queue.set(value.targetId, { value, tail: null });
        return;
    }
    const prevTask = queue.get(value.targetId)!;
    pushTail(prevTask, value);
}

function dequeueTask (queue: WaitingTasksQueue, targetId: number) {
    if (!queue.has(targetId)) {
        return null;
    }

    const { tail, value } = queue.get(targetId)!;
    queue.delete(targetId);
    if (tail) {
        queue.set(targetId, tail);
    }

    return value;
}

// #endregion

// #region Promise util
type ResolvablePromise = [Promise<void>, () => void];
function createResolvablePromise(): ResolvablePromise {
    let externalResolve: () => void = () => null;

    const promise = new Promise<void>(resolve => {
        externalResolve = resolve;
    });

    return [promise, externalResolve];
}
// #endregion
