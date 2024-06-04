import { Actor } from 'apify';
import { CheerioCrawler, sleep } from 'crawlee';

import { pushDataMaxAware } from './push-data.js';

await Actor.init();

const proxyConfiguration = await Actor.createProxyConfiguration();

const crawler = new CheerioCrawler({
    proxyConfiguration,
    requestHandler: async () => {
        await sleep(500);

        const { shouldStop } = await pushDataMaxAware([{ item: 1 }, { item: 2 }, { item: 3 }]);
        if (shouldStop) {
            await crawler.autoscaledPool!.abort();
        }
    },
});

const startRequests = [];

for (let i = 0; i < 1000; i++) {
    startRequests.push({ url: `https://example.com/${i}` });
}

await crawler.run(startRequests);

await Actor.exit();
