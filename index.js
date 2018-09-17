import { server as HapiServer } from 'hapi';
import { config } from 'dotenv';
import metadata from './metadata/pact_indicators.json';
import { getAndSendData } from './helpers/data.helper';
config();
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
  port: 3000,
  host: 'localhost'
});

const init = async () => {
  await server.start();
  const caregiver_program_indicators = metadata.filter(({ programId }) => programId === CAREGIVER_PROGRAM_ID);
  const ovc_program_indicators = metadata.filter(({ programId }) => programId === OVC_PROGRAM_ID);
  console.log(`Server running at: ${server.info.uri}`);
  await getAndSendData(
    ovc_program_indicators,
    PACT_BASE_URL,
    PACT_USER_NAME,
    PACT_PASSWORD,
    MVC_BASE_URL,
    MVC_USER_NAME,
    MVC_PASSWORD
  );
};

process.on('unhandledRejection', err => {
  console.log(err);
  process.exit(1);
});

init();
