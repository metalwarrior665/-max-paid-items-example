import { Actor } from 'apify';

// We use top-level variables with a closure so we don't have to initialize anything
// and can use pushData as drop-in replacement
// We also don't persist anything to KV because we load the state from source of truth (dataset itemCount)
const MAX_ITEMS: number | undefined = Number(process.env.ACTOR_MAX_PAID_DATASET_ITEMS) || undefined;

let isInitialized = false;
let isGettingItemCount = false;
let pushedItemCount = 0;

export const pushDataMaxAware = async (data: Parameters<Actor['pushData']>[0]): Promise<{ shouldStop: boolean }> => {
    // If this isn't pay-per-result, we just push like normally
    if (!MAX_ITEMS) {
        await Actor.pushData(data);
        return { shouldStop: false };
    }

    // We initialize on the first call so that we can use it as standalone function
    // Only the first handler calling pushData() will initialize the count
    if (!isInitialized && !isGettingItemCount) {
        isGettingItemCount = true;
        const dataset = await Actor.openDataset();
        const { itemCount } = (await dataset.getInfo())!;
        pushedItemCount = itemCount;
        isGettingItemCount = false;
        isInitialized = true;
    }

    // Others handlers will wait until initialized which should be few milliseconds anyway
    while (!isInitialized) {
        await new Promise((resolve) => setTimeout(resolve, 50));
    }

    const dataAsArray = Array.isArray(data) ? data : [data];
    const dataToPush = dataAsArray.slice(0, MAX_ITEMS - pushedItemCount);

    if (dataToPush.length) {
        // We have to update the state before 'await' to avoid race conditions
        pushedItemCount += dataToPush.length;
        await Actor.pushData(dataToPush);
    }

    return { shouldStop: pushedItemCount >= MAX_ITEMS };
};
