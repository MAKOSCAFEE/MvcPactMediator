import { chunkedWards } from '../metadata/wards';
import { ouMapper, dataElementMapper } from '../metadata/mvc_pact_mapper';
import got from 'got';

let NUMBER = 0;
export const getAndSendData = async (
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
  for (const chunkWards of chunkedWards) {
    const responsePromises = chunkWards.map(wardid =>
      getPactData(source_base_url, wardid, indicatorIds, source_username, source_password)
    );
    const response = await Promise.all(responsePromises);
    await new Promise(resolve => setTimeout(resolve, 1000));
    const rows = response.map(({ body }) => body).filter(body => body.rows.length);
    if (rows.length) {
      wardsData.push(rows);
    }
  }

  if (wardsData.length) {
    const formatedData = [].concat.apply([], wardsData).map(analytics => formatDataReceived(analytics));
    const dataValues = [].concat.apply([], formatedData);
    const response = await sendDestinationData(
      destination_base_url,
      dataValues,
      destination_username,
      destination_password
    );
    console.log(JSON.stringify((response.body && response.body.importCount) || {}));
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

const getData = async (source_base_url, wardid, indicatorIds, source_username, source_password, callBackFn) => {
  try {
    const { body: wardData } = await getPactData(
      source_base_url,
      wardid,
      indicatorIds,
      source_username,
      source_password
    );
    NUMBER = NUMBER + 1;
    console.log('wardData', NUMBER);
    callBackFn(null, wardData);
  } catch (error) {
    callBackFn(error, null);
  }
};

const getPactData = async (baseUrl, wardId, indicatorIds, username, password) => {
  const authString = new Buffer(`${username}:${password}`).toString('base64');
  const Authorization = `Basic ${authString}`;
  const client = got.extend({
    baseUrl,
    headers: {
      Authorization
    }
  });
  const PACT_ANALYTICS_URL = `api/analytics.json?dimension=dx:${indicatorIds}&dimension=pe:201809&dimension=ou:${wardId};LEVEL-5&displayProperty=NAME&skipMeta=true`;
  return client.get(PACT_ANALYTICS_URL, { json: true });
};

const sendDestinationData = async (baseUrl, dataValues, username, password) => {
  const authString = new Buffer(`${username}:${password}`).toString('base64');
  const Authorization = `Basic ${authString}`;
  const DESTINATION_DATASET_URL = `api/dataValueSets.json`;
  const client = got.extend({
    baseUrl,
    headers: {
      Authorization
    }
  });
  return client.post(DESTINATION_DATASET_URL, { body: { dataValues }, json: true });
};
