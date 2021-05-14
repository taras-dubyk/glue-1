'use strict';
const aws = require('aws-sdk');
const logHelper = require('./logHelper');
const schemaHelper = require('./schemaHelper');
const fs = require('fs');
const https = require('https');

this.glueInstance = null;
const { setDependencies, dependencies } = require('./appDependencies');

module.exports = {
	connect: async (connectionInfo, logger, cb, app) => {
		setDependencies(app);
		const { accessKeyId, secretAccessKey, region, sessionToken } = connectionInfo;
		const sslOptions = await getSslOptions(connectionInfo);
		const httpOptions = sslOptions.ssl ? {
			httpOptions: {
				agent: new https.Agent({
					rejectUnauthorized: true,
					...sslOptions
				})},
				...sslOptions
			} : {};
		aws.config.update({ accessKeyId, secretAccessKey, region, sessionToken, ...httpOptions });

		const glueInstance = new aws.Glue();
		cb(glueInstance);
	},

	disconnect: function(connectionInfo, cb){
		cb();
	},

	testConnection: function(connectionInfo, logger, cb, app) {
		setDependencies(app);
		logInfo('Test connection', connectionInfo, logger);
		const connectionCallback = async (glueInstance) => {
			try {
				await glueInstance.getDatabases().promise();
				cb();
			} catch (err) {
				logger.log('error', { message: err.message, stack: err.stack, error: err }, 'Connection failed');
				cb(err);
			}
		};

		this.connect(connectionInfo, logger, connectionCallback, app);
	},

	getDbCollectionsNames: function(connectionInfo, logger, cb, app) {
		setDependencies(app);
		const connectionCallback = async (glueInstance) => {
			this.glueInstance = glueInstance;
			try {
				const dbsData = await glueInstance.getDatabases().promise();
				const dbsCollections = dbsData.DatabaseList.map(async db => {
					const dbCollectionsData = await glueInstance.getTables({ DatabaseName: db.Name }).promise();
					const dbCollections = dbCollectionsData.TableList.map(({ Name }) => Name);
					return {
						dbName: db.Name,
						dbCollections,
						isEmpty: dbCollections.length === 0
					};
				});
				const result = await Promise.all(dbsCollections);
				cb(null, result);
			} catch(err) {
				logger.log(
					'error',
					{ message: err.message, stack: err.stack, error: err },
					'Retrieving databases and tables information'
				);
				cb(err);
			}
		};

		logInfo('Retrieving databases and tables information', connectionInfo, logger);
		this.connect(connectionInfo, logger, connectionCallback, app);
	},

	getDbCollectionsData: function(data, logger, cb, app) {
		setDependencies(app);
		logger.log('info', data, 'Retrieving schema', data.hiddenKeys);
		
		const { collectionData } = data;
		const databases = collectionData.dataBaseNames;
		const tables = collectionData.collections;

		const getDbCollections = async () => {
			try {
				const tablesDataPromise = databases.map(async dbName => {
					const db = await this.glueInstance.getDatabase({ Name: dbName }).promise();
					const dbDescription = db.Database.Description;
					const selectedDBTables = tables[dbName] || [];
					const dbTables = selectedDBTables.map(async tableName => {
						const rawTableData = await this.glueInstance
							.getTable({ DatabaseName: dbName, Name: tableName })
							.promise();
						logger.progress({
							message: 'Getting table data',
							containerName: dbName,
							entityName: tableName
						});
						return mapTableData(rawTableData, dbDescription);
					});
					return await Promise.all(dbTables);
				});
				
				const tablesData = await Promise.all(tablesDataPromise);
				const flatTablesData = tablesData.reduce((acc, val) => acc.concat(val), []);
				cb(null, flatTablesData);
			} catch(err) {
				logger.log(
					'error',
					{ message: err.message, stack: err.stack, error: err },
					'Retrieving databases and tables information'
				);
				cb({ message: err.message, stack: err.stack });
			}
		};

		getDbCollections();
	}
};

const mapTableData = ({ Table }, dbDescription) => {
	const classification = getClassification(Table.Parameters);
	const partitionKeys = Table.PartitionKeys || [];
	const tableData = {
		dbName: Table.DatabaseName,
		collectionName: Table.Name,
		bucketInfo: {
			description: dbDescription
		},
		entityLevel: {
			description: Table.Description,
			externalTable: Table.TableType === 'EXTERNAL_TABLE',
			tableProperties: mapTableProperties(Table.Parameters),
			compositePartitionKey: partitionKeys.map(item => item.Name),
			compositeClusteringKey: Table.StorageDescriptor.BucketColumns,
			sortedByKey: mapSortColumns(Table.StorageDescriptor.SortColumns),
			compressed: Table.StorageDescriptor.Compressed,
			location: Table.StorageDescriptor.Location,
			numBuckets: Table.StorageDescriptor.NumberOfBuckets,
			storedAsTable: 'input/output format',
			StoredAsSubDirectories: Table.StorageDescriptor.StoredAsSubDirectories,
			inputFormatClassname: Table.StorageDescriptor.InputFormat,
			outputFormatClassname: Table.StorageDescriptor.OutputFormat,
			serDeLibrary: getSerDeLibrary(Table.StorageDescriptor.SerdeInfo),
			parameterPaths: mapSerDePaths(Table.StorageDescriptor.SerdeInfo),
			serDeParameters: mapSerDeParameters(Table.StorageDescriptor.SerdeInfo.Parameters),
			classification
		},
		documents: [],
		validation: {
			jsonSchema:	getColumns([...Table.StorageDescriptor.Columns, ...partitionKeys])
		}
	};
	return tableData;
}

const getColumns = (columns) => {
	return columns.reduce((acc, item) => {
		const sanitizedTypeString = item.Type.replace(/\s/g, '');
		let columnSchema = schemaHelper.getJsonSchema(sanitizedTypeString);
		schemaHelper.setProperty(item.Name, columnSchema, acc);
		return acc;
	}, {});
}

const mapSortColumns = (items) => {
	return items.map(item => ({
		name: item.Column,
		type: item.SortOrder === 1 ? 'ascending' : 'descending'
	}));
}

const getSerDeLibrary = (data = {}) => {
	return data.SerializationLibrary;
}

const mapSerDePaths = (data = {}) => {
	return dependencies.lodash.get(data, 'Parameters.paths', '').split(',');
}

const mapSerDeParameters = (parameters = {}) => {
	return Object.entries(parameters).reduce((acc, [key, value]) => {
		if (key !== 'paths') {
			acc.push({ serDeKey: key, serDeValue: value });
			return acc;
		}
		return acc;
	}, []);
}

const logInfo = (step, connectionInfo, logger) => {
	logger.clear();
	logger.log('info', logHelper.getSystemInfo(connectionInfo.appVersion), step);
	logger.log('info', connectionInfo, 'connectionInfo', connectionInfo.hiddenKeys);
};

const getClassification = (parameters = {}) => {
	if (parameters.classification) {
		switch (parameters.classification.toLowerCase()) {
			case 'avro':
				return 'Avro';
			case 'csv':
				return 'CSV';
			case 'json':
				return 'JSON';
			case 'xml':
				return 'XML';
			case 'parquet':
				return 'Parquet';
			case 'orc':
				return 'ORC';
		}
	}
	return {};
}

const mapTableProperties = (parameters = {}) => {
	return Object.entries(parameters).reduce((acc, [key, value]) => {
		if (key === 'classification') {
			return acc;
		}
		return acc.concat({
			tablePropKey: key,
			tablePropValue: value
		});
	}, []);
}

const readCertificateFile = path => {
	if (!path) {
		return Promise.resolve('');
	}

	return new Promise(resolve => {
		fs.readFile(path, 'utf8', (err, data) => {
			if (err) {
				resolve('');
			}
			resolve(data);
		});
	});
};

const getSslOptions = async connectionInfo => {
	switch (connectionInfo.sslType) {
		case 'Server validation': {
			const certAuthority = await readCertificateFile(connectionInfo.certAuthorityPath);
			return {
				ssl: true,
				ca: [certAuthority],
			};
		}
		case 'Server and client validation': {
			const certAuthority = await readCertificateFile(connectionInfo.certAuthorityPath);
			const key = await readCertificateFile(connectionInfo.clientPrivateKey);
			const cert = await readCertificateFile(connectionInfo.clientCert);
			return {
				ssl: true,
				ca: [certAuthority],
				key: [key],
				cert: [cert],
				passphrase: connectionInfo.clientKeyPassword,
			};
		}
		default:
			return { ssl: false };
	}
};
