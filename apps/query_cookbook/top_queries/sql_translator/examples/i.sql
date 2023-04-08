-- Copyright 2020 Google LLC
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
--     https://www.apache.org/licenses/LICENSE-2.0
--
--     Unless required by applicable law or agreed to in writing, software
--     distributed under the License is distributed on an "AS IS" BASIS,
--     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
--     See the License for the specific language governing permissions and
--     limitations under the License.
--

SELECT
  1+2 AS f,
  'alksdjf asdfiojasdfl kasjfd lkasjdf dlaksjdf laksjdf laksjdf lkasjdf alskdfjalksdfja lskdfja lskdfjasl dflkasjd falksdjf' as c,
  [1,2,3] as g,
  [SomeDuperLongFunction('abc'),
  SomeDuperLongFunction('abc1'),
  SomeDuperLongFunction('abc2'),
  SomeDuperLongFunction('abc3'),
  SomeDuperLongFunction('abc4'),
  SomeDuperLongFunction('abc5'),
  SomeDuperLongFunction('abc6'),
  SomeDuperLongFunction('abc7'),
  SomeDuperLongFunction('abc8'),
  SomeDuperLongFunction('abc9'),
  SomeDuperLongFunction('abc10'),
  SomeDuperLongFunction('abc11'),
  SomeDuperLongFunction('abc12'),
  SomeDuperLongFunction('abc13'),
  SomeDuperLongFunction('abc14')] AS h
FROM
  a
WHERE 
 'asdlkfjasd lfkjas dflkdajs dfdlkasjd fa' = 'asldkfjas lkdfdja slfdkdja sldfkja sldkfajsdf ';
