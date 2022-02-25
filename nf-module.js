import { config, common } from '@nfjs/core';
import { dataProviders } from '@nfjs/back';
import { authProviders } from '@nfjs/auth';
import AuthProvider from './lib/auth-provider.js';
import { provider as Provider } from './lib/provider.js';

const meta = {
    require: {
        after: '@nfjs/back',
    }
};

async function init() {
    Object.keys(config.data_providers || []).forEach((provider) => {
        if (config.data_providers[provider].type === 'db-postgres') {
            dataProviders[provider] = new Provider(config.data_providers[provider] || {}, provider);
        }
    });

    const moduleConfig = common.getPath(config, '@nfjs/db-postgres');
    if (moduleConfig && moduleConfig.useSimpleDbPostgresAuth) {
        const _authProvider = new AuthProvider();
        authProviders['db-postgres'] = _authProvider;
    }
}

export {
    init,
    meta,
};
