\set library_file '\''`pwd`'/udx.R\''
\set display_library_file '\''`pwd`'/vertica-udx-display.R\''


CREATE OR REPLACE LIBRARY vertica_ddR_display AS :display_library_file LANGUAGE 'R';
CREATE OR REPLACE TRANSFORM FUNCTION ddR_display_object AS NAME 'vertica.ddR.display.factory' LIBRARY vertica_ddR_display;
--CREATE OR REPLACE TRANSFORM FUNCTION ddR_test_varbinary AS NAME 'vertica.varbinary.test.factory' LIBRARY vertica_ddR_display;



CREATE OR REPLACE LIBRARY vertica_ddR AS :library_file LANGUAGE 'R';
CREATE OR REPLACE TRANSFORM FUNCTION ddR_worker AS NAME 'vertica.ddR.worker.factory' LIBRARY vertica_ddR;
CREATE OR REPLACE TRANSFORM FUNCTION ddR_serialize AS NAME 'vertica.ddR.serialize.factory' LIBRARY vertica_ddR;
