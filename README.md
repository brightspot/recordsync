# RecordSync

This extension provides the ability for Brightspot to automatically and incrementally export records from one instance and import them into another.

* [Prerequisites](#prerequisites)
* [Installation](#installation)
* [Usage](#usage)
* [Documentation](#documentation)
* [Versioning](#versioning)
* [Contributing](#contributing)
* [Local Development](#local-development)
* [License](#license)

## Prerequisites

This extension requires an instance of [Brightspot](https://www.brightspot.com/) running at least Java 11 and access to the project source code.

## Installation

Gradle:
```groovy
api 'com.brightspot:recordsync:1.0.0'
```

Maven:
```xml
<dependency>
    <groupId>com.brightspot</groupId>
    <artifactId>recordsync</artifactId>
    <version>1.0.0</version>
</dependency>
```

Substitute `1.0.0` for the desired version found on the [releases](../../releases) list.

## Usage

This document assumes you have already created a secure storage bucket (e.g. AWS S3) and have the necessary credentials to access it. Reference the [Brightspot documentation to configure this StorageItem](https://www.brightspot.com/documentation/brightspot-cms-developer-guide/latest/configuring-storageitem), and take note of the `name` of the `StorageItem` configuration.

This extension is configured in the `context.xml` file. There are two different configurations: one for the "Exporter" and one for the "Importer."

You can configure multiple exporters and importers by changing `primary` in the configuration keys to a different name.

Some of the configuration options are required, while others are optional. The optional configurations have reasonable defaults, but you may want to adjust them based on your specific needs.

The storage name and path prefix are required for both the exporter and importer configurations, and they must point to the same bucket and path.

### Best Practices

This extension is intended to incrementally sync records between two environments when one is a copy of the other. Exports are only saved for a brief period of time (see `dataRetentionHours`), and imports are only performed on records that are newer than the last export. 

One example of a good use case is syncing records from a production environment to a lower environment. In this case, the exporter would be configured on the production environment, and the importer would be configured on the lower environment.

Before this extension runs for the first time, it is recommended to start with a fresh "copy-down" of the production data to the lower environment database. Use the date of the copy-down as the `earliestDate` in the exporter configuration. This will ensure that only records created or modified after the copy-down are exported, which will save time, computing resources, bandwidth, and storage space.

### Exporter Configuration
```xml
<?xml version="1.0" encoding="utf-8"?>
<Context>
    <!-- Schedule the exporter to run at your desired interval. This example is 4:25 AM every day. Required. -->
    <Environment name="brightspot/recordsync/exporters/primary/cron"
                 value="0 25 4 * * ?"
                 type="java.lang.String"/>
    <!-- The name of the storage bucket as configured elsewhere in this file. See: dari/storage/*. Required. -->
    <Environment name="brightspot/recordsync/exporters/primary/storage"
                 value="recordsync"
                 type="java.lang.String"/>
    <!-- The path prefix ("folder") that all records will be exported to in the storage bucket. Required. -->
    <Environment name="brightspot/recordsync/exporters/primary/pathPrefix"
                 value="recordsync"
                 type="java.lang.String"/>
    <!-- The hostname or IP address of the host that is responsible for running this export. Required. -->
    <Environment name="brightspot/recordsync/exporters/primary/taskHost"
                 value="task-server.local"
                 type="java.lang.String"/>
    <!-- The maximum size in MB of each export file (before compression).
         Note that each entire file is processed in memory one at a time, so keep this relatively small.
         The default is 100 MB. -->
    <Environment name="brightspot/recordsync/exporters/primary/maxFileSizeMB"
                 value="100"
                 type="java.lang.String"/>
    <!-- The batchSize is used as the LIMIT clause for the database query, and is also the maximum
         number of records per file. The default is 5000. -->
    <Environment name="brightspot/recordsync/exporters/primary/batchSize"
                 value="5000"
                 type="java.lang.String"/>
    <!-- The number of hours to retain each exported file. The default is 168 (one week). -->
    <Environment name="brightspot/recordsync/exporters/primary/dataRetentionHours"
                 value="168"
                 type="java.lang.String"/>
    <!-- A comma-separated list of fully-qualified type names to exclude from exports. There are
         already reasonable defaults in place (the example below is NOT the default!); this adds to that set. Optional. -->
    <Environment name="brightspot/recordsync/exporters/primary/excludedTypes"
                 value="com.psddev.cms.db.ToolEntity,com.psddev.cms.db.SiteSettings,com.psddev.sitemap.SiteMap,com.psddev.sitemap.SiteMapPartition"
                 type="java.lang.String"/>
    <!-- A comma-separated list of fully-qualified type names in case you _only_ want to export those types. Optional. -->
    <Environment name="brightspot/recordsync/exporters/primary/includedTypes"
                 value=""
                 type="java.lang.String"/>
    <!-- Records older than this timestamp will not be exported. Optional, but *strongly* recommended. -->
    <Environment name="brightspot/recordsync/exporters/primary/earliestDate"
                 value="1999-12-31T23:59:59Z"
                 type="java.lang.String"/>
</Context>
```

### Importer Configuration
```xml
<?xml version="1.0" encoding="utf-8"?>
<Context>
    <!-- Schedule the importer to run at your desired interval. This example is 5:25 AM every day. Required. -->
    <Environment name="brightspot/recordsync/importers/primary/cron"
                 value="0 25 5 * * ?"
                 type="java.lang.String"/>
    <!-- The name of the storage bucket as configured elsewhere in this file. See: dari/storage/*. Required. -->
    <Environment name="brightspot/recordsync/importers/primary/storage"
                 value="recordsync"
                 type="java.lang.String"/>
    <!-- The path prefix ("folder") that all records will be imported from in the storage bucket. Required. -->
    <Environment name="brightspot/recordsync/importers/primary/pathPrefix"
                 value="recordsync"
                 type="java.lang.String"/>
    <!-- The hostname or IP address of the host that is responsible for running this import. Required. -->
    <Environment name="brightspot/recordsync/importers/primary/taskHost"
                 value="task-server.local"
                 type="java.lang.String"/>
    <!-- Maximum number of records to import in a single transaction; The default is 500. -->
    <Environment name="brightspot/recordsync/importers/primary/batchSize"
                 value="500"
                 type="java.lang.String"/>
    <!-- A comma-separated list of fully-qualified type names to exclude from imports. There are
         already reasonable defaults in place (the example below is NOT the default!); this adds to that set.
         If the types have already been excluded on the exporter, there is no need to exclude them on the importer. Optional. -->
    <Environment name="brightspot/recordsync/importers/primary/excludedTypes"
                 value="com.psddev.cms.db.ToolEntity,com.psddev.cms.db.SiteSettings,com.psddev.sitemap.SiteMap,com.psddev.sitemap.SiteMapPartition"
                 type="java.lang.String"/>
    <!-- A comma-separated list of fully-qualified type names in case you _only_ want to import those types. Optional. -->
    <Environment name="brightspot/recordsync/importers/primary/includedTypes"
                 value=""
                 type="java.lang.String"/>
    <!-- Records older than this timestamp will not be imported. If the exporter 
    has already set an earliestDate, this one is unnecessary. Optional. -->
    <Environment name="brightspot/recordsync/exporters/primary/earliestDate"
                 value="1999-12-31T23:59:59Z"
                 type="java.lang.String"/>
</Context>
```

## Documentation

- [Javadocs](https://artifactory.psdops.com/public/com/brightspot/recordsync/%5BRELEASE%5D/recordsync-%5BRELEASE%5D-javadoc.jar!/index.html)

## Versioning

The version numbers for this extension will strictly follow [Semantic Versioning](https://semver.org/).

## Contributing

If you have feedback, suggestions or comments on this open-source platform extension, please feel free to make them publicly on the issues tab [here](https://github.com/brightspot/recordsync/issues).

Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.

## Local Development

Assuming you already have a local Brightspot instance up and running, you can
test this extension by running the following command from this project's root
directory to install a `SNAPSHOT` to your local Maven repository:

```shell
./gradlew -Prelease=local publishToMavenLocal
```

Next, ensure your project's `build.gradle` file contains

```groovy
repositories {
    mavenLocal()
}
```

Then, add the following to your project's `build.gradle` file:

```groovy
dependencies {
    api 'com.brightspot:recordsync:local'
}
```

Finally, compile your project and run your local Brightspot instance.

## License

See: [LICENSE](LICENSE).
