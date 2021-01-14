/* Copyright 2020 Google, LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
	
package com.google.cloud.sa.tagengine.service.zeta;

import com.google.cloud.functions.HttpFunction;
import com.google.cloud.functions.HttpRequest;
import com.google.cloud.functions.HttpResponse;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.zetasql.Analyzer;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;
import java.util.logging.Logger;

public class ZetaSqlParserFunction implements HttpFunction {
    private static final Logger logger = Logger.getLogger(ZetaSqlParserFunction.class.getName());
    private static final Gson gson = new Gson();

    String ERROR = "{\"error\":\"unable to parse sql statement\"";

    @Override
    public void service(HttpRequest request, HttpResponse response)
            throws IOException {

        String result;
        // Parse JSON request and check for "name" field
        try {
            JsonElement requestParsed = gson.fromJson(request.getReader(), JsonElement.class);
            JsonObject requestJson = null;

            if (requestParsed != null && requestParsed.isJsonObject()) {
                requestJson = requestParsed.getAsJsonObject();
            }

            var sql = requestJson.get("sql").getAsString();
            List<List<String>> tables = Analyzer.extractTableNamesFromStatement(sql);
            result = gson.toJson(tables);

        } catch (Exception e) {
            logger.severe("Error parsing JSON: " + e.getMessage());
            result = ERROR;
        }

        var writer = new PrintWriter(response.getWriter());
        writer.printf(result);
    }
}





