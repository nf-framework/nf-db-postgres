import assert from 'assert';
import { describe, it } from 'mocha';
import AbortController from 'node-abort-controller';

import { config } from '@nfjs/core';

import * as testing from '../provider.js';

describe('@nfjs/db-postgres/lib/provider', () => {
    const cfg = config?.data_providers?.default;
    const cfgTest = config?.['@nfjs/db-postgres']?.test ?? config?.test;
    const provider = new testing.provider(cfg);
    describe('query()', () => {
        it('check aborting', async () => {
            // Arrange
            const connect = await provider.getConnect(cfgTest);
            const controller = new AbortController();
            const signal = controller.signal;
            // Act
            setTimeout(() => {
                controller.abort();
            }, 1000);
            try {
                const queryRes = await provider.query(connect, 'SELECT pg_sleep(:sec)', { sec: 10 },{ signal }, {});
                assert.strictEqual(1, 0);
            } catch (e) {
                const errorCode = e?.__stack?.[0]?.detail?.code;
                // Assert
                assert.strictEqual(errorCode, '57014');
            } finally {
                if (connect) provider.releaseConnect(connect);
            }
        });
        it('check options.returnFirst = true', async () => {
            // Arrange
            let connect;
            try {
                const connect = await provider.getConnect(cfgTest);
            // Act
                const queryRes = await provider.query(connect, 'select true as f1, 1 as f2', {},{ rowMode: 'object', returnFirst: true}, {});
            // Assert
                assert.strictEqual(queryRes.data?.f2, 1);
            } finally {
                if (connect) provider.releaseConnect(connect);
            }
        });
        it('check options.returnFirst = false (default)', async () => {
            // Arrange
            let connect;
            try {
                const connect = await provider.getConnect(cfgTest);
                // Act
                const queryRes = await provider.query(connect, 'select true as f1, 1 as f2', {},{ rowMode: 'object' }, {});
                // Assert
                assert.strictEqual(queryRes.data?.[0]?.f2, 1);
            } finally {
                if (connect) provider.releaseConnect(connect);
            }
        });
    });
    describe('func()', () => {
        const f = 'create function public.providerfunctest(p_f1 text, p_f2 text, p_f3 text default \'\') returns text language sql as $$select p_f1||p_f2||p_f3;$$;';
        it('check missing params', async () => {
            // Arrange
            let connect;
            try {
                connect = await provider.getConnect(cfgTest);
                await provider.startTransaction(connect);
                await connect.query(f);
                // Act
                try {
                    const queryRes = await provider.func(connect, 'public.providerfunctest', {f1: '1'});
                    assert.strictEqual(1, 0);
                } catch (e) {
                    assert.match(e.message, /f2/);
                }
                //
                // Assert
                await provider.rollback(connect);
            } finally {
                if (connect) provider.releaseConnect(connect);
            }
        });
    });
});
