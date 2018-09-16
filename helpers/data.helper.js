import { wards } from '../metadata/wards';
import got from 'got';

export const getAndSendData = async (indicators, source_base_url, source_username, source_password) => {
  const indicatorIds = indicators.map(({ id }) => id).join(';');
  const { body: wardData } = await getPactData(
    source_base_url,
    wards[0],
    indicatorIds,
    source_username,
    source_password
  );
  console.log(wardData);
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
  const PACT_ANALYTICS_URL = `api/analytics.json?dimension=dx:${indicatorIds}&dimension=pe:THIS_MONTH&dimension=ou:${wardId};LEVEL-5&displayProperty=NAME&skipMeta=true`;
  return client.get(PACT_ANALYTICS_URL, { json: true });
};

const sendDestinationData = async (baseUrl, data, username, password) => {
  const authString = new Buffer(`${username}:${password}`).toString('base64');
  const Authorization = `Basic ${authString}`;
  const client = got.extend({
    baseUrl,
    headers: {
      Authorization
    }
  });
};
