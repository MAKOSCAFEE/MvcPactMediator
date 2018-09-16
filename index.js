import { server as HapiServer } from 'hapi';
import { config } from 'dotenv';
import metadata from './metadata/pact_indicators.json';
config();
const { CAREGIVER_PROGRAM_ID, OVC_PROGRAM_ID } = process.env;

const server = HapiServer({
  port: 3000,
  host: 'localhost'
});

const init = async () => {
  await server.start();
  const caregiver_program_indicators = metadata.filter(({ programId }) => programId === CAREGIVER_PROGRAM_ID);
  const ovc_program_indicators = metadata.filter(({ programId }) => programId === OVC_PROGRAM_ID);
  console.log(`Server running at: ${server.info.uri}`);
};

process.on('unhandledRejection', err => {
  console.log(err);
  process.exit(1);
});

init();
