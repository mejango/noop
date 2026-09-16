import path from 'path';
import { readProfile } from '../../../integrations/derive-v3/profile';
import { assertIsolatedDataPaths } from '../../../integrations/derive-v3/isolation';

export const VENUE = readProfile();
export const IS_V3 = VENUE.version === 3;

assertIsolatedDataPaths(VENUE, process.env, path.resolve(process.cwd(), '..'));
