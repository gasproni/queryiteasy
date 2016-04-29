package com.asprotunity.queryiteasy.acceptance_tests;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;

public abstract class TestPropertiesLoader {
    public static Properties loadProperties(Path path) throws IOException {
        Properties result = new Properties();
        try (InputStream is = new FileInputStream(path.toFile())) {
            result.load(is);
            return result;
        }
    }

    static Path prependTestDatasourcesConfigFolderPath(String propertiesFileName) {
        return Paths.get("test_datasources_config", propertiesFileName);
    }
}
