const { server: HapiServer } = require('hapi');
const { config } = require('dotenv');
const metadata = require('./metadata/pact_indicators.json');
const { dataElementMapper } = require('./metadata/mvc_pact_mapper');
const { chunkArray } = require('./helpers/utils');
const { getAndSendData } = require('./helpers/data.helper');
config();

let indicatorIndex = 0;
const {
  CAREGIVER_PROGRAM_ID,
  OVC_PROGRAM_ID,
  PACT_BASE_URL,
  MVC_BASE_URL,
  PACT_USER_NAME,
  PACT_PASSWORD,
  MVC_USER_NAME,
  MVC_PASSWORD
} = process.env;

const server = HapiServer({
  port: 3018,
  host: 'localhost'
});

const init = async () => {
  await server.start();
  const caregiver_program_indicators = metadata.filter(
    ({ programId, id }) => programId === CAREGIVER_PROGRAM_ID && Object.keys(dataElementMapper).includes(id)
  );
  const ovc_program_indicators = metadata.filter(
    ({ programId, id }) => programId === OVC_PROGRAM_ID && Object.keys(dataElementMapper).includes(id)
  );
  console.log(`Server running at: ${server.info.uri}`);
  const chunkedOVCPrIndicators = chunkArray(ovc_program_indicators, 5);

  for (const chunkedIndicatorArray of chunkedOVCPrIndicators) {
    indicatorIndex += 1;
    console.log(`chunked number  ${indicatorIndex}`);
    await getAndSendData(
      chunkedIndicatorArray,
      PACT_BASE_URL,
      PACT_USER_NAME,
      PACT_PASSWORD,
      MVC_BASE_URL,
      MVC_USER_NAME,
      MVC_PASSWORD
    );
  }

  const chunkedCaregiverPrIndicators = chunkArray(caregiver_program_indicators, 20);

  for (const chunkedIndicatorArray of chunkedCaregiverPrIndicators) {
    indicatorIndex += 1;
    console.log(`chunked number  ${indicatorIndex}`);
    await getAndSendData(
      chunkedIndicatorArray,
      PACT_BASE_URL,
      PACT_USER_NAME,
      PACT_PASSWORD,
      MVC_BASE_URL,
      MVC_USER_NAME,
      MVC_PASSWORD
    );
  }
  console.log(`THE END`);
  process.exit();
};

process.on('unhandledRejection', err => {
  console.log(err);
  process.exit(1);
});

init();
