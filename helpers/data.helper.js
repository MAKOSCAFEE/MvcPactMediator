import { wards, testWards } from '../metadata/wards';
import { ouMapper, dataElementMapper } from '../metadata/mvc_pact_mapper';
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
    async (error, results) => {
      const resultsWithData = results.filter(({ rows }) => rows.length);
      if (resultsWithData.length) {
        const formatedData = resultsWithData.map(analytics => formatDataReceived(analytics));
        const dataValues = [].concat.apply([], formatedData);
        const retunedData = await sendDestinationData(
          destination_base_url,
          dataValues,
          destination_username,
          destination_password
        );
        console.log(retunedData);
      } else {
        process.end(1);
      }
    }
  );
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
      value: row[valueIndex]
    };
  });
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
  const PACT_ANALYTICS_URL = `api/analytics.json?dimension=dx:${indicatorIds}&dimension=pe:201805&dimension=ou:${wardId};LEVEL-5&displayProperty=NAME&skipMeta=true`;
  return client.get(PACT_ANALYTICS_URL, { json: true });
};

const sendDestinationData = async (baseUrl, dataValues, username, password) => {
  const authString = new Buffer(`${username}:${password}`).toString('base64');
  const Authorization = `Basic ${authString}`;
  console.log(JSON.stringify({ dataValues }));
  const DESTINATION_DATASET_URL = `api/dataValueSets.json`;
  const client = got.extend({
    baseUrl,
    headers: {
      Authorization
    }
  });
  return client.post(DESTINATION_DATASET_URL, { body: { dataValues }, json: true });
};
