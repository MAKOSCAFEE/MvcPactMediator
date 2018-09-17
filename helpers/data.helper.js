import { wards, testWards } from '../metadata/wards';
import { parallelLimit } from 'async';
import got from 'got';

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

  parallelLimit(
    testWards.map(wardId => async callBackFn => {
      await new Promise(resolve => setTimeout(resolve, 1000));
      return getData(source_base_url, wardId, indicatorIds, source_username, source_password, callBackFn);
    }),
    10,
    (error, results) => {
      const resultsWithData = results.filter(({ rows }) => rows.length);
      const formatedData = resultsWithData.map(analytics => formatDataReceived(analytics));
      const dataValues = [].concat.apply([], formatedData);
      // await sendDestinationData(destination_base_url,dataValues,destination_username,destination_password);
    }
  );
};

const formatDataReceived = data => {
  const { headers, rows } = data;
  const dxIndex = headers.findIndex(({ name }) => name === 'dx');
  const peIndex = headers.findIndex(({ name }) => name === 'pe');
  const ouIndex = headers.findIndex(({ name }) => name === 'ou');
  const valueIndex = headers.findIndex(({ name }) => name === 'value');
  const formatedRows = rows.map(row => ({
    dataElement: row[dxIndex],
    categoryOptionCombo: 'cocID',
    period: row[peIndex],
    orgUnit: row[ouIndex],
    value: row[valueIndex]
  }));
  return formatedRows;
};

const getData = async (source_base_url, wardid, indicatorIds, source_username, source_password, callBackFn) => {
  const { body: wardData } = await getPactData(source_base_url, wardid, indicatorIds, source_username, source_password);

  callBackFn(null, wardData);
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
  const PACT_ANALYTICS_URL = `api/analytics.json?dimension=dx:${indicatorIds}&dimension=pe:THIS_YEAR&dimension=ou:${wardId};LEVEL-5&displayProperty=NAME&skipMeta=true`;
  return client.get(PACT_ANALYTICS_URL, { json: true });
};

const sendDestinationData = async (baseUrl, dataValues, username, password) => {
  const authString = new Buffer(`${username}:${password}`).toString('base64');
  const Authorization = `Basic ${authString}`;
  const DESTINATION_DATASET_URL = `api/dataValueSets`;
  const client = got.extend({
    baseUrl,
    headers: {
      Authorization
    }
  });
  return client.post(DESTINATION_DATASET_URL, { body: { dataValues }, json: true });
};
