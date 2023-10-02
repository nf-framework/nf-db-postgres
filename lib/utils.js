import { config, common, message as msgFormatter } from '@nfjs/core';

import { dbsrc } from '@nfjs/back';

const debugMsgsExtendedInfo = common.getPath(config, 'debug.msgsExtendedInfo') || false;
const typeBackConvertToPrimitive = {
    1043: { primitive: 'text' }, // string/varchar
    23: { primitive: 'numb' }, // integer/int4
    21: { primitive: 'numb' }, // smallint/int2
    20: { primitive: 'numb' }, // bigint/int8
    26: { primitive: 'text' }, // oid
    1700: { primitive: 'numb' }, // numeric
    700: { primitive: 'numb' }, // real/float4
    701: { primitive: 'numb' }, // double precision / float 8
    16: { primitive: 'bool' }, // boolean
    1184: { primitive: 'date', sub: 'timestamptz' }, // timestamptz
    1114: { primitive: 'date', sub: 'timestamp' }, // timestamp
    1082: { primitive: 'date', sub: 'date' }, // date
    869: { primitive: 'text' }, // inet
    650: { primitive: 'text' }, // cidr
    829: { primitive: 'text' }, // macaddr
    3906: { primitive: 'text' }, // numrange
    1186: { primitive: 'text' }, // interval
    17: { primitive: 'text' }, // bytea
    1000: { primitive: 'text' }, // array/boolean
    1014: { primitive: 'text' }, // array/char
    1015: { primitive: 'text' }, // array/varchar
    1008: { primitive: 'text' }, // array/text
    1001: { primitive: 'text' }, // array/bytea
    1231: { primitive: 'text' }, // array/numeric
    1005: { primitive: 'text' }, // array/int2
    1007: { primitive: 'text' }, // array/int4'
    1016: { primitive: 'text' }, // array/int8'
    1017: { primitive: 'text' }, // array/point
    1028: { primitive: 'text' }, // array/oid
    1021: { primitive: 'text' }, // array/float4
    1022: { primitive: 'text' }, // array/float8
    1182: { primitive: 'text' }, // array/date
    1041: { primitive: 'text' }, // array/inet
    651: { primitive: 'text' }, // array/cidr
    1040: { primitive: 'text' }, // array/macaddr
    3907: { primitive: 'text' }, // array/numrange
    25: { primitive: 'text' }, // binary-string
    600: { primitive: 'text' }, // point
    718: { primitive: 'text' }, // circle
    114: { primitive: 'json' }, // json
    3802: { primitive: 'json' }, // jsonb
};

/**
 * Конвертация типа данных базы данных в псевдотипы для интерфейса
 * @param {number} dataType - тип данных
 * @return {{primitive: string, sub?: string}}
 */
function convertTypeBackToPrimitive(dataType) {
    return typeBackConvertToPrimitive[dataType] || { primitive: 'text' };
}

/**
 * Приведение запроса из вида 'select :param1' в 'select $1'
 * @param {Query} query - экземпляр класса запроса, подготовленный
 * @param {{sql: string, params: *[], missedParams: string[]}} frmQuery - обработанный запрос
 */
function formatQuery(query, frmQuery) {
    // замена :param на $1 синтаксис запроса
    let ind = 1;
    const tmpParams = {};
    Object.keys(query.params).forEach((key) => { tmpParams[key] = null; });
    const clearSql = query.sql.replace(/(['][^']*?['])/g, (all, g1) => g1.replace(/:/g, '\u205A'));
    const regex = new RegExp('([^:])(:)(\\w+)', 'ig');
    frmQuery.sql = clearSql.replace(regex, (all, g1, g2, g3) => {
        if (g3 in tmpParams) {
            if (tmpParams[g3] === null) {
                frmQuery.params.push(query.params[g3]);
                tmpParams[g3] = `$${ind}`;
                return `${g1}$${ind++}`;
            }
            return g1 + tmpParams[g3];
        }
        frmQuery.missedParams.push(g3);
        return all;
    }).replace(/\u205A/g, ':');
}

/**
 * Формирование строкового описания для таблицы
 * @param {string} schema - схема таблицы
 * @param {string} table - имя таблицы
 * @return {Promise<string>}
 */
async function getTableDescr(schema, table) {
    const tableMeta = await dbsrc.getTable(schema, table);
    let res;
    if (tableMeta && tableMeta.comment) {
        const { comment, schema: _schema } = tableMeta;
        if (debugMsgsExtendedInfo) {
            res = `${comment}(${_schema}.${table})`;
        } else {
            res = comment;
        }
    } else {
        res = `${schema}.${table}`;
    }
    return res;
}

/**
 * Формирование строкового описания для набора колонок
 * @param {string} schema - схема таблицы
 * @param {string} table - имя таблицы
 * @param {string} columns - перечень имен колонок, разделенных запятой
 * @return {Promise<string>}

 */
async function getColumnsDescr(schema, table, columns) {
    const tableMeta = await dbsrc.getTable(schema, table);
    let res = columns;
    if (tableMeta && tableMeta.cols) {
        res = columns.split(',').map((clmn) => {
            const _clmn = tableMeta.cols.find((tc) => tc.name === clmn);
            const comment = _clmn && _clmn.comment;
            if (comment) {
                if (debugMsgsExtendedInfo) {
                    return `${comment}(${clmn})`;
                }
                return comment;
            }
            return clmn;
        }).join(',');
    }
    return res;
}

/**
 * Формирование человеко-читаемого сообщения об ошибке, возникшей в базе данных
 * @param {Error} e
 * @return {Promise<string>}
 */
async function formatError(e) {
    let msg;
    const { code, message } = e;
    try {
        // https://www.postgresql.org/docs/current/errcodes-appendix.html
        // unique_violation
        if (code === '23505') {
            const { constraint, detail, schema, table } = e;
            // detail : Key (caption)=(asd) already exists.
            const [, clmns, dupValues] = detail.match(/\((.*?)\)=\((.*?)\)/);
            const tableDescr = await getTableDescr(schema, table);
            const clmnsDescr = await getColumnsDescr(schema, table, clmns);
            const replaces = [tableDescr, clmnsDescr, dupValues, `${schema}.${table}.${constraint}`];
            msg = msgFormatter.getMsg(`${schema}.${table}.${constraint}`, 'dbWarn', replaces);
            if (!msg) msg = msgFormatter.getMsg('unique_violation', 'dbWarnCommon', replaces);
        // not_null_violation
        } else if (code === '23502') {
            const { column, schema, table } = e;
            const tableDescr = await getTableDescr(schema, table);
            const clmnDescr = await getColumnsDescr(schema, table, column);
            const replaces = [tableDescr, clmnDescr];
            msg = msgFormatter.getMsg(`${schema}.${table}.${column}#notnull`, 'dbWarn', replaces);
            if (!msg) msg = msgFormatter.getMsg('not_null_violation', 'dbWarnCommon', replaces);
        // foreign_key_violation
        } else if (code === '23503') {
            const { constraint, detail, schema, table } = e;
            const tableDescr = await getTableDescr(schema, table);
            let mode;
            const lowMessage = message.toLowerCase();
            if (lowMessage.indexOf('update') !== -1 && lowMessage.indexOf('delete') !== -1) {
                mode = 'ud';
            } else if (lowMessage.indexOf('insert') !== -1 && lowMessage.indexOf('update') !== -1) {
                mode = 'iu';
            }
            if (mode === 'ud') {
                // detail : На ключ (id)=(2) всё ещё есть ссылки в таблице "role_unitprivs".
                // message : UPDATE или DELETE в таблице "roles" нарушает ограничение внешнего ключа "fk4role_unitprivs8role_id" таблицы "role_unitprivs"
                const [, value] = detail.match(/\)=\((.*)\)/);
                const [, curTablename] = message.match(/"(.*?)"/);
                const curTableDescr = await getTableDescr(undefined, curTablename);
                const replaces = [curTableDescr, tableDescr, value, `${schema}.${table}.${constraint}`];
                msg = msgFormatter.getMsg(`${schema}.${table}.${constraint}#ud`, 'dbWarn', replaces);
                if (!msg) msg = msgFormatter.getMsg('foreign_key_violation#ud', 'dbWarnCommon', replaces);
            }
            if (mode === 'iu') {
                // detail : Ключ (role_id)=(800) отсутствует в таблице "roles".
                // message : INSERT или UPDATE в таблице "userroles" нарушает ограничение внешнего ключа "fk4userroles8role_id"
                const [, clmn, value] = detail.match(/\((.*?)\)=\((.*?)\)/);
                const [, foreignTablename] = detail.match(/"(.*?)"/);
                const foreignTableDescr = await getTableDescr(undefined, foreignTablename);
                const clmnDescr = await getColumnsDescr(schema, table, clmn);
                const replaces = [tableDescr, clmnDescr, foreignTableDescr, value, `${schema}.${table}.${constraint}`];
                msg = msgFormatter.getMsg(`${schema}.${table}.${constraint}#iu`, 'dbWarn', replaces);
                if (!msg) msg = msgFormatter.getMsg('foreign_key_violation#iu', 'dbWarnCommon', replaces);
            }
            if (!msg) msg = msgFormatter.getMsg(`${schema}.${table}.${constraint}`, 'dbWarn', [detail]);
            if (!msg) msg = msgFormatter.getMsg('foreign_key_violation', 'dbWarnCommon', [detail]);
        // check_violation
        } else if (code === '23514') {
            const { constraint, schema, table } = e;
            // detail : Failing row contains (67, null, asdшш).
            const replaces = [`${schema}.${table}.${constraint}`];
            msg = msgFormatter.getMsg(`${schema}.${table}.${constraint}`, 'dbWarn', replaces);
            if (!msg) msg = msgFormatter.getMsg('check_violation', 'dbWarnCommon', replaces);
        // exclusion_violation
        } else if (code === '23P01') {
            const { constraint, schema, table } = e;
            // detail : Key conflicts with existing key.
            const replaces = [`${schema}.${table}.${constraint}`];
            msg = msgFormatter.getMsg(`${schema}.${table}.${constraint}`, 'dbWarn', replaces);
            if (!msg) msg = msgFormatter.getMsg('exclusion_violation', 'dbWarnCommon', replaces);
        // ошибка вызванная напрямую из кода pl\pgsql
        } else if (code === 'P0001') {
            const { where } = e;
            // если в формате подбора сообщения
            try {
                const { msgcode, namespace, replaces } = JSON.parse(message);
                if (msgcode) {
                    msg = msgFormatter.getMsg(msgcode, namespace, replaces);
                }
            } catch (er) {
            }
            if (!msg) msg = message;
        } else {
            msg = message;
        }
    } catch (err) {
        msg = `Ошибка [${err.message}] при разборе сообщения от базы данных: ${message}`;
    }
    return msg;
}

/**
 * Формирование отладочной информации при обработке и выполнении запроса
 * @param {Query} query - запрос прошедший обработку классом Query
 * @param {{sql: string, params: *[], missedParams: string[]}} frmQuery - обработанный запрос
 * @param {Array<*>} initialArguments - оригинальные параметры вызова метода провайдера
 * @param {Object} timing - метрики производительности
 * @return {Object}
 */
function getDebug(query, frmQuery, initialArguments, timing) {
    return {
        execQuery: frmQuery.sql,
        execParams: frmQuery.params,
        ctrlLocateQuery: query.locate_sql,
        ctrlQuery: query.rawsql,
        ctrlParams: query.rawparams || {},
        ctrlControl: query.rawcontrol || {},
        initQuery: initialArguments[1],
        initParams: initialArguments[2] || {},
        initControl: initialArguments[3] || {},
        timing: { provider: timing },
    };
}

export {
    formatQuery,
    formatError,
    getDebug,
    convertTypeBackToPrimitive,
};
