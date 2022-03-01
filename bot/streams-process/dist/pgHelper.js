export function buildInList(values, startIdx) {
    startIdx = startIdx ? startIdx : 1;
    const result = [];
    for (let i = 0; i < values.length; ++i) {
        result.push('$' + (startIdx + i));
    }
    return result;
}
export function buildMultiInsert(stmt, template, data, mapping) {
    // split the template
    const templateParts = template.split(/\$\d+/);
    // need at least subcount columns in a row
    const subcount = templateParts.length - 1;
    const params = [];
    const values = [];
    for (let i = 0; i < data.length; ++i) {
        let row = data[i];
        if (mapping) {
            try {
                row = mapping(data[i]);
            }
            catch (e) {
                throw new Error(`unable to map: ${data[i]}`);
            }
        }
        if (row.length < subcount) {
            throw new Error('not enough parameters, need ' + subcount);
        }
        let chunk = templateParts[0];
        for (let j = 0; j < subcount; ++j) {
            values.push(row[j]);
            chunk += '$' + values.length + templateParts[j + 1];
        }
        params.push('(' + chunk + ')');
    }
    return {
        text: stmt + params.join(', '),
        values,
    };
}
