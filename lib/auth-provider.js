import { dataProviders } from '@nfjs/back';

class AuthProvider {
    /**
     * Попытка аутентификации пользователя средствами провайдера данных 'default'
     * @param {string} user
     * @param {string} password
     * @param {SessionAPI} session
     * @returns {Promise<{result: boolean, detail?: string}>}
     */
    async login(user, password, session) {
        const provider = this._getProvider();
        const credentials = { user, password };
        let ret;
        let connect;
        try {
            connect = await provider.getConnect(credentials);
            session.assign('context', { user });
            ret = { result: true };
        } catch (err) {
            ret = { result: false, detail: err.message };
        } finally {
            if (connect) provider.releaseConnect(connect);
        }
        return ret;
    }

    /**
     * Выход пользователя
     * @param {SessionAPI} session
     * @return {Promise<void>}
     */
    async logout(session) {
        session.destroy();
    }

    /** @private */
    _getProvider() {
        return dataProviders.default;
    }
}

export default AuthProvider;
