const { chunkedWards } = require('../metadata/wards');
const { ouMapper, dataElementMapper } = require('../metadata/mvc_pact_mapper');
const got = require('got');

const MONTH_MAPPING = {
  0: '01',
  1: '02',
  2: '03',
  3: '04',
  4: '05',
  5: '06',
  6: '07',
  7: '08',
  8: '09',
  9: '10',
  10: '11',
  11: '12'
};

let NUMBER = 0;
exports.getAndSendData = async (
  indicators,
  source_base_url,
  source_username,
  source_password,
  destination_base_url,
  destination_username,
  destination_password
) => {
  const indicatorIds = indicators.map(({ id }) => id).join(';');

  const wardsData = [];
  const todayDate = new Date();
  todayDate.setMonth(todayDate.getMonth() - 1);
  const period = `${todayDate.getFullYear()}${MONTH_MAPPING[todayDate.getMonth()]}`;
  for (const chunkWards of chunkedWards) {
    const responsePromises = chunkWards.map(wardid =>
      // remove the hardcoded period after this.
      getPactData(source_base_url, wardid, indicatorIds, source_username, source_password, '201811')
    );
    const response = await Promise.all([].concat.apply([], responsePromises));
    await new Promise(resolve => setTimeout(resolve, 1000));
    const rows = response.map(({ body }) => body).filter(body => body.rows.length);
    if (rows.length) {
      wardsData.push(rows);
      const formatedData = [].concat.apply([], rows).map(analytics => formatDataReceived(analytics));
      const dataValues = [].concat.apply([], formatedData);
      const response = await sendDestinationData(
        destination_base_url,
        dataValues,
        destination_username,
        destination_password
      );
      const { ignored } = response.body && response.body.importCount;
      if (ignored) {
        console.log(JSON.stringify(response.body.conflicts || response.body.importCount));
      } else {
        console.log(JSON.stringify((response.body && response.body.importCount) || {}));
      }
    }
  }

  return wardsData;
};

const formatDataReceived = data => {
  const { headers, rows } = data;
  const dxIndex = headers.findIndex(({ name }) => name === 'dx');
  const peIndex = headers.findIndex(({ name }) => name === 'pe');
  const ouIndex = headers.findIndex(({ name }) => name === 'ou');
  const valueIndex = headers.findIndex(({ name }) => name === 'value');
  const formatedRows = rows.map(row => {
    const [dataElement, categoryOptionCombo] = dataElementMapper[row[dxIndex]].split('.');
    return {
      dataElement,
      categoryOptionCombo,
      period: row[peIndex],
      orgUnit: ouMapper[row[ouIndex]] || row[ouIndex],
      value: Math.round(row[valueIndex])
    };
  });
  return formatedRows;
};

const getPactData = async (baseUrl, wardId, indicatorIds, username, password, period) => {
  const authString = Buffer.from(`${username}:${password}`).toString('base64');
  const Authorization = `Basic ${authString}`;
  const client = got.extend({
    baseUrl,
    headers: {
      Authorization
    },
    timeout: 250000,
    retry: 5
  });
  const PACT_ANALYTICS_URL = `api/analytics.json?dimension=dx:${indicatorIds}&dimension=pe:${period}&dimension=ou:LEVEL-5;${wardId}&displayProperty=NAME&skipMeta=true`;
  return client.get(PACT_ANALYTICS_URL, { json: true });
};

const sendDestinationData = async (baseUrl, dataValues, username, password) => {
  const authString = Buffer.from(`${username}:${password}`).toString('base64');
  const Authorization = `Basic ${authString}`;
  const DESTINATION_DATASET_URL = `api/dataValueSets.json`;
  const client = got.extend({
    baseUrl,
    headers: {
      Authorization
    },
    timeout: 250000,
    retry: 5
  });
  return client.post(DESTINATION_DATASET_URL, { body: { dataValues }, json: true });
};
