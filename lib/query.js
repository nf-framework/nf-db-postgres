import { common } from '@nfjs/core';

/**
 * Преобразование запроса из текста, параметров и настроек до готового к выполнению запроса
 * @property {string} rawsql - изначальный текст запроса
 * @property {Object} rawparams - изначально переданные параметры
 * @property {ProviderQueryControl} rawcontrol - изначальные настройки запроса
 * @property {string} sql - итоговый запрос
 * @property {Object} params - итоговые параметры
 * @property {*} connect - открытый коннект к базе данных
 * @property {string} mainsql - основная часть итогового запроса, без сортировок и границ
 * @property {boolean} _dontApplyRange - признак, что в запрос не будут включены условия на границ
 * @property {string} sort_expr - сформированный текст по всем нужным сортировкам
 * @property {string} locate_sql - сформированный текст запроса на вычисление первичного ключа и позиции(номера строки) позиционируемой строки в данных
 * @property {number} located_rn - номер позиционируемой строки
 * @property {boolean} chunk - признак, что итоговый запрос будет на часть данных (limit,offset)
 * @property {number} chunk_start - номер начала части отдаваемых данных
 * @property {number} chunk_real_start - номер начала части отдаваемых данных с запасом на 1 строку назад, если отдается часть не сначала данных
 * @property {number} chunk_end - номер окончания части отдаваемых данных
 */
class Query {
    constructor(sql, params, control, connect) {
        this.rawsql = sql;
        this.rawparams = params;
        this.rawcontrol = control;
        this.sql = sql;
        // подразумевается, что параметры всегда в виде объекта
        this.params = ((typeof params === 'object') ? common.cloneDeep(params) : {});
        this.connect = connect;
    }

    /**
     * Применение всех манипуляций с запросом
     */
    async setControl() {
        if (this.rawcontrol) {
            this.setFilter();
            // основа запроса, которая будет использована для locate
            if (this.rawcontrol.count === true) {
                this.setCount();
                this.mainsql = this.sql;
                return;
            }
            this.mainsql = this.sql;
            this.setSort();
            if (!this._dontApplyRange) await this.setRange();
        }
    }

    /**
     * Встраивание фильтров в запрос
     */
    setFilter() {
        const { filters } = this.rawcontrol;
        const res_filter = [];
        const selectFields = ['flt.*'];
        if (Array.isArray(filters) && filters.length > 0) {
            const res_filter_params = {};
            const operator_in_value_1 = ['~', '>', '<', '=', '!', '[', ']'];
            const operators_1 = ['~', '>', '<', '=', '!=', '>=', '<='];
            const operator_in_value_2 = ['>=', '<=', '!=', '!~'];
            // отобрать только валидные фильтры
            filters.filter((f) => f && f.field).forEach((filter_item, index) => {
                if (!filter_item) return;
                // fieldtype
                let field_b = '';
                let field_e = '';
                let param_b = '';
                let param_e = '';
                let filter;
                let cast = filter_item.cast || '';
                const field = filter_item.field;
                let value = filter_item.value;
                let operator = filter_item.operator;
                if ((cast && !cast.match(/^(left\-|right\-)?[:a-z0-9\[\]]+$/i)) ||
                    !field.match(/^[a-z0-9_]+$/i)) {
                    return;
                }
                if (!operator) {
                    if (value === '()') {
                        filter = `flt.${field} is null`;
                    } else if (value === '!()') {
                        filter = `flt.${field} is not null`;
                    }
                }
                if (!operator && !filter) {
                    const index = operator_in_value_1.indexOf(value[0]);
                    if (index !== -1) {
                        operator = operators_1[index];
                        value = value.substring(1);
                    }
                    if (!operator) {
                        const index2 = operator_in_value_2.indexOf(value.substring(0, 2));
                        if (index2 !== -1) {
                            operator = operator_in_value_2[index2];
                            value = value.substring(2);
                        }
                    }
                }
                if ((!operator || operator == 'like_both') && !filter) {
                    cast = 'lower';
                    field_e = '::text';
                    if (operator == 'like_both') {
                        param_b = "'%'||";
                    }
                    operator = 'like';
                    param_e = "::text||'%'";
                }

                if (['~', '~*', '>', '<', '=', '!=', '<>', '>=', '<=', 'like', 'ilike', '@>', '<@'].indexOf(operator) === -1) return;

                switch (filter_item.fieldtype) {
                    case 'N':
                        param_e = '::numeric';
                        break;
                    case 'D':
                        param_b = 'to_date(';
                        param_e = '::text, \'dd.mm.yyyy\'::text)';
                        break;
                    default:
                        break;
                }
                if (cast && cast.startsWith('right-')) {
                    cast = cast.slice(6);
                    if (cast.startsWith('::')) {
                        param_e += cast;
                    } else {
                        field_b = `${field_b}`;
                        field_e += '';
                        param_b = `${cast}(${param_b}`;
                        param_e += ')';
                    }
                } else if (cast && cast.startsWith('left-')) {
                    cast = cast.slice(5);
                    if (cast.startsWith('::')) {
                        field_e += cast;
                    } else {
                        field_b = `${cast}(${field_b}`;
                        field_e += ')';
                        param_b = `${param_b}`;
                        param_e += '';
                    }
                } else if (cast) {
                    if (cast.startsWith('::')) {
                        field_e += cast;
                        param_e += cast;
                    } else {
                        field_b = `${cast}(${field_b}`;
                        field_e += ')';
                        param_b = `${cast}(${param_b}`;
                        param_e += ')';
                    }
                }

                if ((operator === 'like' || operator === 'ilike') && param_e.indexOf("'%'") === -1)
                    param_e = param_e + "||'%'";

                if (filter) {
                    res_filter.push(filter);
                } else if ((operator === '=' || operator === '!=') && (value === null)) {
                    res_filter.push(`flt.${field} is${(operator === '=') ? '' : ' not'} null`);
                } else {
                    res_filter.push(`${field_b}flt.${field}${field_e} ${operator} ${param_b}:fltr${index}_${field}${param_e}`);
                    this.params[`fltr${index}_${field}`] = value;
                }
            });
        }
        const tree_filter = [];
        if (this.rawcontrol.treeMode) {
            const ctrl = this.rawcontrol.treeMode;
            if (ctrl.hidField && ctrl.hidValue !== undefined) {
                if (!ctrl.hidField.match(/^[a-z0-9_]+$/i)) {
                    return;
                }
                if (ctrl.hidValue == null) {
                    tree_filter.push(`${ctrl.hidField} is null`);
                } else {
                    tree_filter.push(`${ctrl.hidField} = :fltr_tm_${ctrl.hidField}`);
                    this.params[`fltr_tm_${ctrl.hidField}`] = ctrl.hidValue;
                }
            }

            if (res_filter.length === 0) {
                if (!ctrl.keyField.match(/^[a-z0-9_]+$/i)) {
                    return;
                }
                const hasChildSQL = `, exists (select 1 from (${this.sql}) q where q.${ctrl.hidField} = flt.${ctrl.keyField}) as _hasChildren`;
                this.sql = `
                    select ${selectFields.join(',')}${!ctrl.hasChildField ? hasChildSQL : ''} 
                    from (${this.sql}) as flt ${tree_filter.length > 0 ? 'where ' : ''} ${tree_filter.join(' and ')}`;
            } else {
                const hasChildSQL = `, exists (select 1 from paths q where q.${ctrl.hidField} = flt.${ctrl.keyField}) as _hasChildren`;
                this.sql = `
                with recursive main as not materialized (${this.sql}),
                filtered as (select * from main flt where ${res_filter.join(' and ')}),
                paths  as (
                    select f.* from filtered f
                    union 
                    select m.* from main m,paths p where p.${ctrl.hidField} = m.${ctrl.keyField}
                )
                select flt.* ${!ctrl.hasChildField ? hasChildSQL : ''}  from paths flt`;
                if (ctrl.filterByHid === true) {
                    this.sql += ` ${tree_filter.length > 0 ? 'where ' : ''} ${tree_filter.join(' and ')}`;
                } else {
                    this._dontApplyRange = true;
                }
            }
        } else if (res_filter.length > 0) {
            this.sql = `select ${selectFields.join(',')} from (${this.sql}) as flt where ${res_filter.join(' and ')}`;
        }
    }

    setCount() {
        this.sql = `select count(*) as _count_ from (${this.sql}) as c `;
    }

    /**
     * Встраивание сортировок в запрос
     */
    setSort() {
        const sorts = this.rawcontrol.sorts;
        if (!(Array.isArray(sorts) && sorts.length > 0)) {
            if (this.rawcontrol.datamode === 'scroll' || this.rawcontrol.datamode === 'tree') {
                this.sql = `${this.sql} order by 1`;
            }
            return;
        }
        this.sort_expr = sorts.filter((s) => s.field.match(/^[a-z0-9_]+$/i))
            .map((s) => s.field + ((s.sort === 'desc') ? ' desc nulls last' : ''));
        if (this.sort_expr.length > 0) {
            this.sql = `${this.sql} order by ${this.sort_expr.join(',')}`;
        }
    }

    /**
     * Встраивание лимитирования данных
     */
    async setRange() {
        const range = this.rawcontrol.range,
            locate = this.rawcontrol.locate;
        if (range && range.chunk_start >= 0) {
            this.sql = `${this.sql} limit :lmt_limit_ offset :lmt_offset_`;
            this.chunk = true;
            if (locate && locate.field && locate.locating) {
                if (!locate.field.match(/^[a-z0-9_]+$/i)) {
                    return;
                }

                this.locate_sql = `select pos - 1 as pos from (select ROW_NUMBER() over (order by ${(this.sort_expr) ? this.sort_expr : '1'}) pos, ${locate.field} from (${this.mainsql}) main) rn where ${locate.field} = :locate_value_`;
                const tmp_params = { ...this.params };
                tmp_params.locate_value_ = locate.value;
                const result_locate = await this.connect.query(this.locate_sql, tmp_params, {});
                if (Array.isArray(result_locate.data) && result_locate.data[0] !== undefined) { // есть на чем позиционировать
                    this.located_rn = parseInt(result_locate.data[0].pos);
                    this.chunk_start = this.located_rn;
                    this.chunk_end = this.chunk_start + ((range.amount) ? range.amount : 10) - 1;
                } else { // иначе сброс на первую страницу
                    this.chunk_start = 0;
                    this.chunk_end = ((range.amount) ? range.amount : 10) - 1;
                }
            } else {
                this.chunk_start = range.chunk_start;
                this.chunk_end = ((range.chunk_end) ? range.chunk_end : range.chunk_start + ((range.amount) ? range.amount : 10) - 1);
            }
            this.chunk_real_start = (this.chunk_start === 0) ? 0 : this.chunk_start - 1;
            this.params.lmt_limit_ = this.chunk_end - this.chunk_real_start + 2;
            this.params.lmt_offset_ = this.chunk_real_start;
        }
    }
}
export default Query;
