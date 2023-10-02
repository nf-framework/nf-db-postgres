import pkg from 'pg';
import { config as _config, api, common, debug, errors, container, instanceName, applicationName } from '@nfjs/core';
import { NFProvider } from '@nfjs/back';
import { convertTypeBackToPrimitive, formatError, formatQuery, getDebug } from './utils.js';
import Query from './query.js';

const { Client, Pool } = pkg;
const funcCache = new Map();


let ignoreTypes = common.getPath(_config, '@nfjs/db-postgres.preventParsingForTypes') || [];

ignoreTypes.forEach(type => {
    pkg.types.setTypeParser(type,  (val) => val);
});

class NFPostgresProvider extends NFProvider {
    constructor(config) {
        super(config);
        const { connectConfig, supportConnectConfig } = this.config;
        if (supportConnectConfig) {
            const supportPoolConfig = { ...connectConfig, ...{ max: 1 }, ...supportConnectConfig };
            this.supportPool = new Pool(supportPoolConfig);
        }
    }

    async getConnect(credentials, options= {}) {
        try {
            const {forceCredentials = false, connectPlace} = options;
            const appName = (!!connectPlace)
                ? connectPlace.replace(/{applicationName}/g, this.config?.connectConfig?.application_name ?? applicationName)
                    .replace(/{instanceName}/g, instanceName)
                : undefined;
            await super.getConnect(credentials, options);
            let {
                connectType, connectConfig, connectPoolConfig, credentialsSource,
            } = this.config;
            const connectCfg = { ...connectConfig };
            if (forceCredentials === true) {
                connectType = 'user';
            }
            if (forceCredentials === true || credentialsSource === 'session') {
                Object.assign(connectCfg, credentials);
            }
            let pool;
            let client;
            if (connectType === 'pool' || connectType === 'poolPerUser') {
                const poolName = (connectType === 'pool') ? '__main' : connectCfg.user;
                if (!this.pools[poolName]) {
                    if (connectPoolConfig) {
                        Object.assign(connectCfg, connectPoolConfig);
                    }
                    pool = new Pool(connectCfg);
                    this.pools[poolName] = { pool, created: process.hrtime(), lastUsed: process.hrtime() };
                } else {
                    ({ pool } = this.pools[poolName]);
                    this.pools[poolName].lastUsed = process.hrtime();
                }
                client = await pool.connect();
            } else {
                client = new Client(connectCfg);
                await client.connect();
            }
            let onConnectQueries = this.onConnectQueries.map((curQuery) => client.query(curQuery.statement, curQuery.params));
            const onConnectConfig = this.onConnectConfig.map(
                (curConfig) => client.query('select pg_catalog.set_config($1,$2,false)', [curConfig.name, curConfig.value])
            );
            onConnectQueries = onConnectQueries.concat(onConnectConfig);
            if (appName) onConnectQueries.push(client.query('select pg_catalog.set_config($1,$2,false)', ['application_name', appName]));
            await Promise.all(onConnectQueries);
            if (container.metrics) {
                container.metrics.increment(container.metrics.counter(`provider_${this.config?.name}_connect_count`));
            }
            return client;
        } catch (e) {
            throw api.nfError(e);
        }
    }

    startTransaction(connect) {
        return connect.query('begin');
    }

    commit(connect) {
        return connect.query('commit');
    }

    rollback(connect) {
        return connect.query('rollback');
    }

    releaseConnect(connect) {
        if (connect) {
            if (container.metrics) {
                container.metrics.increment(container.metrics.counter(`provider_${this.config?.name}_release_count`));
            }
            if ('release' in connect) {
                // когда коннект был через пул соединений
                return connect.release();
            }
            return connect.end();
        }
        return Promise.resolve();
    }

    async query(connect, sql, params, options, control) {
        function _getDebug(_query, frmQuery, args, tmng) {
            if (common.getPath(_config, 'debug.need')) return getDebug(_query, frmQuery, args, tmng);
            return {};
        }
        const defaultOpts = { rowMode: 'array', returnRN: false, returnFirst: false };
        let { rowMode, returnRN, signal, returnFirst } = { ...defaultOpts, ...options };
        const tmng = {};
        const _query = new Query(sql, params, control, connect);
        const frmQuery = { sql: '', params: [], missedParams: [] };
        try {
            debug.timingStart(tmng, 'control');
            await _query.setControl();
            debug.timingEnd(tmng, 'control');
            formatQuery(_query, frmQuery);
        } catch (e) {
            // eslint-disable-next-line prefer-rest-params
            throw api.nfError(e, NFProvider.getMsg('parseQueryFailed'), _getDebug(_query, frmQuery, arguments));
        }
        // проверка что не все параметры были переданы
        if (frmQuery.missedParams.length > 0) {
            let _msg = [...new Set(frmQuery.missedParams)]; // чтобы оставить только уникальные
            _msg = `${NFProvider.getMsg('notAllParamsPassed')}: ${_msg.join(',')}`;
            // eslint-disable-next-line prefer-rest-params
            throw api.nfError(undefined, _msg, _getDebug(_query, frmQuery, arguments));
        }
        try {
            // выбран такой режим выполнения запроса, потому что в нем происходит биндинг параметров,
            // а не вклеивание в запрос литералов(хоть и безопасное)
            debug.timingStart(tmng, 'execute');
            const pgQuery = new Client.Query({
                text: frmQuery.sql,
                rowMode,
            }, frmQuery.params);
            // функционал отмены выполняемого (или запланированного) запроса
            if (signal) {
                signal.addEventListener('abort', async () => {
                    if (this.supportPool) {
                        if (connect.activeQuery === pgQuery) {
                            let supportConnect;
                            try {
                                supportConnect = await this.supportPool.connect();
                                await supportConnect.query('select pg_cancel_backend($1)', [connect.processID]);
                            } finally {
                                supportConnect.release();
                            }
                        } else if (connect.queryQueue.indexOf(pgQuery) !== -1) {
                            connect.queryQueue.splice(connect.queryQueue.indexOf(pgQuery), 1);
                        }
                    }
                });
            }
            let queryResult = new Promise((resolve, reject) => {
                pgQuery.callback = (err, res) => (err ? reject(err) : resolve(res));
            });
            connect.query(pgQuery);
            queryResult = await queryResult;
            debug.timingEnd(tmng, 'execute');
            if (Array.isArray(queryResult)) { // было несколько операторов выполнено возвращаем данные последнего
                queryResult = queryResult[queryResult.length - 1];
            }
            const fieldsMetadata = queryResult.fields.map((currentField) => {
                const { primitive, sub } = convertTypeBackToPrimitive(currentField.dataTypeID);
                return {
                    name: currentField.name,
                    dataType: primitive,
                    dataSubType: sub
                };
            });
            const fieldsCount = fieldsMetadata.length;
            if (returnRN) fieldsMetadata[fieldsCount] = { name: '_rn', dataType: 'numb' };
            const response = {
                metaData: fieldsMetadata,
                rowMode
            };
            if ('located_rn' in _query) response.located = _query.located_rn;
            if ('chunk' in _query) {
                response.chunk = _query.chunk;
                response.chunk_start = _query.chunk_start;
                response.chunk_end = _query.chunk_end;
            }
            // eslint-disable-next-line prefer-rest-params
            const _dbg = _getDebug(_query, frmQuery, arguments, tmng);
            if (_dbg) response.debug = _dbg;
            if (returnFirst) queryResult.rows = [queryResult.rows?.[0]];
            if (rowMode === 'array') {
                const data = queryResult.rows;
                if (returnRN) {
                    const rowNumberStart = (_query.chunk_real_start) ? _query.chunk_real_start : 0;
                    data.forEach((curr, index) => {
                        curr[fieldsCount] = rowNumberStart + index;
                    });
                }
                response.data = data;
                return response;
            }
            const data = queryResult.rows;
            if (returnRN) {
                const rowNumberStart = (_query.chunk_real_start) ? _query.chunk_real_start : 0;
                data.forEach((curr, index) => {
                    curr._rn = rowNumberStart + index;
                });
            }
            response.data = (returnFirst) ? data?.[0] : data;
            return response;
        } catch (e) {
            const msg = await this.formatError(e);
            // eslint-disable-next-line prefer-rest-params
            throw api.nfError(e, `${msg}`, _getDebug(_query, frmQuery, arguments));
        }
    }

    async func(connect, func, params) {
        /*
        Выполнение функции в бд с биндингом совпадающих реальных параметров функции в виде именованного вызова
        Например при func = public.fnc и params = {pn_id:null,ps_str:"Пример",ps_str2:"Несуществующий параметр в функции"}
        итоговый запрос в бд будет в виде: select * from public.fnc(pn_id := $1, ps_str := $2) , который вернет данные в
        виде именованного json, если функция возвращает именованные out параметры или таблицу с именованными колонками, а
        иначе json объект с именем result
        */
        function _getDebug(frmQuery, frmParams, timing) {
            if (common.getPath(_config, 'debug.need')) {
                return {
                    execQuery: frmQuery,
                    execParams: frmParams || {},
                    initQuery: func,
                    initParams: params || {},
                    timing: { provider: timing },
                };
            }
            return {};
        }
        let frmSql;
        let frmParams;
        const tmng = {};
        try {
            debug.timingStart(tmng, 'meta');
            let funcRealParams;
            if (funcCache.has(func)) {
                funcRealParams = funcCache.get(func);
            } else {
                const queryRes = await connect.query(
                    `select t.parameter_name,
                          t.udt_name,
                          t.parameter_mode,
                          case when t.parameter_default is null and t.parameter_mode in ('IN','INOUT')  then true 
                              else false end as required  
                     from information_schema.parameters t 
                          join pg_namespace tpn on tpn.nspname = t.udt_schema 
                          join pg_type tp on (tp.typnamespace = tpn.oid and tp.typname = t.udt_name) 
                    where t.specific_schema = $1 and t.specific_name ~ $2`,
                    [func.split('.')[0], `^${func.split('.')[1]}_[0-9]+$`],
                );
                funcRealParams = queryRes?.rows;
                funcCache.set(func, funcRealParams);
            }
            debug.timingEnd(tmng, 'meta');
            frmSql = `select * from ${func}(`;
            let i = 1;
            frmParams = [];
            const missedParams = [];
            funcRealParams.forEach(((v) => {
                let funcParam;
                let keyFromParams;
                if (v.parameter_name in params) {
                    funcParam = v.parameter_name;
                    keyFromParams = v.parameter_name;
                } else {
                    keyFromParams = Object.keys(params).find(curParam => v.parameter_name === `p_${curParam}`);
                    if (keyFromParams) funcParam = v.parameter_name;
                }
                if (funcParam) {
                    frmSql += `${funcParam}:=$${i++}::${v.udt_name},`;
                    frmParams.push(params[keyFromParams]);
                } else if (v.required) missedParams.push(v.parameter_name);
            }));
            // проверка что не все параметры были переданы
            if (missedParams.length > 0) {
                throw api.nfError(undefined, `${NFProvider.getMsg('notAllParamsPassed')}: ${missedParams.join(',')}`);
            }
            const commaIndex = frmSql.lastIndexOf(',');
            frmSql = (commaIndex !== -1) ? frmSql.substring(0, commaIndex) : frmSql;
            frmSql += ') result';
            debug.timingStart(tmng, 'execute');
            const resultObj = await connect.query({ text: frmSql, values: frmParams });
            debug.timingEnd(tmng, 'execute');
            const response = { data: resultObj.rows };
            const debugInfo = _getDebug(frmSql, frmParams, tmng);
            if (debug) response.debug = debugInfo;
            return response;
        } catch (e) {
            const debugInfo = _getDebug(frmSql, frmParams);
            let _msg;
            // вызвать форматирование ошибки, только если она напрямую из бд, а не кастомная
            if (!(e instanceof errors.NFError)) _msg = await this.formatError(e);
            throw api.nfError(e, _msg, { debug: debugInfo });
        }
    }

    setContext(connect, context) {
        const { contextSource, contextConfig } = this.config;
        const _context = [...context];
        // если источник для сессионных настроек - конфиг, то перезаписываем значения пришедшие, а недостающие добавляем
        if (contextSource === 'config' && contextConfig) {
            contextConfig.forEach((k) => {
                const _contextIndex = _context.findIndex((i) => i.name === k.name);
                if (_contextIndex === -1) {
                    _context.push(k);
                } else {
                    _context[_contextIndex].value = k.value;
                }
            });
        }
        const contextQueries = _context.map(
            (curConfig) => connect.query({
                text: 'select pg_catalog.set_config($1,$2,false)',
                values: [curConfig.name, curConfig.value]
            })
        );
        return Promise.all(contextQueries);
    }

    getMetaCompletingStatement(component, prefix, tableName) {
        const hasComma = prefix.indexOf('.');
        let res;
        if (hasComma === -1) { // подсказываем только схемы в базе
            res = `select t.schema_name as name,
                          null as description,
                          'schema' as meta 
                     from information_schema.schemata t
                    where t.schema_name like :prefix||'%'
                    order by 1 asc, 3 asc
                    limit :limit`;
        } else if (component === 'action') {
            const splitData = prefix.split('.');
            if (splitData.length === 2) {
                res = `select code as name, null as description, null as meta 
                    from nfc.v4unitlist where code like :prefix||'%'
                    order by 1 asc, 3 asc
                    limit :limit`;
            } else {
                res = `select code as name, null as description, null as meta 
                from nfc.v4unitbps where code like :prefix||'%'
                order by 1 asc, 3 asc
                limit :limit`;
            }
            // res = select * from (
            //        select r.routine_schema||'.'||r.routine_name as name,
            //               null as description,
            //               r.routine_type as meta
            //          from information_schema.routines r
            //         where r.routine_schema = split_part(:prefix,'.',1)
            //           and r.routine_name like split_part(:prefix,'.',2)||'%'
            //        ) as f
            //        order by 1 asc, 3 asc
            //        limit :limit`;
        } else if (component === 'dataset') {
            if (tableName) {
                res = `select c.column_name as name, 
                              'column' as meta,
                              c.udt_name as datatype
                        from information_schema.columns c
                        where c.table_schema = split_part(:tableName,'.',1) and 
                        c.table_name = split_part(:tableName,'.',2)`;
            } else if (prefix.split('.').length === 3) {
                // для текста вида nfc.unitlist.mdl - выдадим таблицу и поле на которое ссылается это поле
                res = `select * from (
                        select t.table_schema||'.'||t.table_name||'.'||t.column_name as name,
                               null as description,
                               'column('||t.data_type||')' as meta
                            from information_schema.columns t
                            where t.table_schema = split_part(:prefix,'.',1)
                            and t.table_name = split_part(:prefix,'.',2)
                            and t.column_name like split_part(:prefix,'.',3)||'%'
                        union all
                        select :prefix||' -> '||(t6.nspname||'.'||t4.relname)::text as name,
                               null as description,
                               'reference table' as meta
                          from pg_catalog.pg_namespace   t1,
                               pg_catalog.pg_class       t2,
                               pg_catalog.pg_constraint  t3,
                               pg_catalog.pg_class       t4,
                               pg_catalog.pg_namespace   t6,
                               pg_catalog.pg_attribute   t5
                         where t1.nspname      = split_part(:prefix,'.',1)
                           and t2.relnamespace = t1.oid 
                           and t2.relname      = split_part(:prefix,'.',2)
                           and t3.conrelid     = t2.oid
                           and t4.oid          = t3.confrelid
                           and t6.oid          = t4.relnamespace
                           and t5.attrelid     = t2.oid
                           and t5.attnum       = any(t3.conkey)
                           and t5.attname      = split_part(:prefix,'.',3)::name
                        ) as f
                        order by 1 asc, 3 asc
                        limit :limit`;
            } else {
                // текст вида nfc.unitlist - список таблиц,представлений,функций в схеме
                res = `select * from (
                        select t.table_schema||'.'||t.table_name as name,
                                null as description,
                                t.table_type as meta
                            from information_schema.tables t
                            where t.table_schema = split_part(:prefix,'.',1)
                            and t.table_name like split_part(:prefix,'.',2)||'%'
                            and t.table_type in ('BASE TABLE','VIEW')
                        union all
                        select r.routine_schema||'.'||r.routine_name as name,
                                null as description,
                                r.routine_type as meta
                            from information_schema.routines r
                            where r.routine_schema = split_part(:prefix,'.',1)
                            and r.routine_name like split_part(:prefix,'.',2)||'%'
                        ) as f
                        order by 1 asc, 3 asc
                        limit :limit`;
            }
        }
        return res;
    }

    formatQuery(source, target) {
        return formatQuery(source, target);
    }

    async formatError(e) {
        return formatError(e);
    }
}
export { NFPostgresProvider as provider };
