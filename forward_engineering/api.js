'use strict'

const aws = require('../reverse_engineering/node_modules/aws-sdk');
const { getDatabaseStatement } = require('./helpers/databaseHelper');
const { getTableStatement } = require('./helpers/tableHelper');
const { getIndexes } = require('./helpers/indexHelper');
const foreignKeyHelper = require('./helpers/foreignKeyHelper');
const { getGlueDatabaseCreateStatement } = require('./helpers/awsCliScriptHelpers/glueDatabaseHeleper');
const { getGlueTableCreateStatement } = require('./helpers/awsCliScriptHelpers/glueTableHelper');
const { getApiStatements } = require('./helpers/awsCliScriptHelpers/applyToInstanceHelper');
const sqlFormatter = require('./custom_modules/sql-formatter');
const { setDependencies, dependencies } = require('./appDependencies');


module.exports = {
	generateScript(data, logger, callback, app) {
		try {
			setDependencies(app);
			const jsonSchema = JSON.parse(data.jsonSchema);
			const modelDefinitions = JSON.parse(data.modelDefinitions);
			const internalDefinitions = JSON.parse(data.internalDefinitions);
			const externalDefinitions = JSON.parse(data.externalDefinitions);
			const containerData = data.containerData;
			const entityData = data.entityData;

			if (data.options.targetScriptOptions && data.options.targetScriptOptions.keyword === 'hiveQl') {
				const needMinify = (
					dependencies.lodash.get(data, 'options.additionalOptions', []).find(
						(option) => option.id === 'minify'
					) || {}
				).value;

				return callback(null, buildHiveScript(needMinify)(
					getDatabaseStatement(containerData),
					getTableStatement(containerData, entityData, jsonSchema, [
						modelDefinitions,
						internalDefinitions,
						externalDefinitions
					]),
					getIndexes(containerData, entityData, jsonSchema, [
						modelDefinitions,
						internalDefinitions,
						externalDefinitions
					])
				));
			}

			const script = buildAWSCLIScript(containerData, jsonSchema);
			return callback(null, script);
		} catch (e) {
			logger.log('error', { message: e.message, stack: e.stack }, 'AWS Glue -Engineering Error');

			setTimeout(() => {
				callback({ message: e.message, stack: e.stack });
			}, 150);
		}
	},

	generateContainerScript(data, logger, callback, app) {
		try {
			setDependencies(app);
			const containerData = data.containerData;
			const modelDefinitions = JSON.parse(data.modelDefinitions);
			const externalDefinitions = JSON.parse(data.externalDefinitions);
			const databaseStatement = getDatabaseStatement(containerData);
			const jsonSchema = parseEntities(data.entities, data.jsonSchema);
			const internalDefinitions = parseEntities(data.entities, data.internalDefinitions);
			if (data.options.targetScriptOptions && data.options.targetScriptOptions.keyword === 'hiveQl') {
				const needMinify = (
					dependencies.lodash.get(data, 'options.additionalOptions', []).find(
						(option) => option.id === 'minify'
					) || {}
				).value;

				const foreignKeyHashTable = foreignKeyHelper.getForeignKeyHashTable(
					data.relationships,
					data.entities,
					data.entityData,
					jsonSchema,
					internalDefinitions,
					[
						modelDefinitions,
						externalDefinitions
					],
					[modelDefinitions, externalDefinitions],
					containerData[0] && containerData[0].isActivated
				);
	
				const entities = data.entities.reduce((result, entityId) => {
					const args = [
						containerData,
						data.entityData[entityId],
						jsonSchema[entityId], [
							internalDefinitions[entityId],
							modelDefinitions,
							externalDefinitions
						]
					];
	
					return result.concat([
						getTableStatement(...args),
						getIndexes(...args),
					]);
				}, []);
	
				const foreignKeys = data.entities.reduce((result, entityId) => {
					const foreignKeyStatement = foreignKeyHelper.getForeignKeyStatementsByHashItem(foreignKeyHashTable[entityId] || {});
				
					if (foreignKeyStatement) {
						return [...result, foreignKeyStatement];
					}
	
					return result;
				}, []).join('\n');
	
				return callback(null, buildHiveScript(needMinify)(
					databaseStatement,
					...entities,
					foreignKeys
				));
			}

			const script = buildAWSCLIModelScript(containerData, jsonSchema);
			return callback(null, script);
		} catch (e) {
			logger.log('error', { message: e.message, stack: e.stack }, 'Cassandra Forward-Engineering Error');

			setTimeout(() => {
				callback({ message: e.message, stack: e.stack });
			}, 150);
		}
	},

	async applyToInstance(data, logger, callback, app) {
		setDependencies(app);
		if (!data.script) {
			return callback({ message: 'Empty script' });
		}

		logger.clear();
		logger.log('info', data, data.hiddenKeys);

		const glueInstance = getGlueInstance(data, app);

		try {
			const { db, table } = getApiStatements(data.script);
			if (db.length === 0 && table.length === 0) {
				return callback({ message: 'HiveQL is not supported for this operation' });
			}
			const dbCreatePromises = db.map(async statement => {
				logger.progress({ message: 'Creating database', containerName: statement.DatabaseInput.Name });
				return await glueInstance.createDatabase(statement).promise();
			});
			await Promise.all(dbCreatePromises);
			const tableCreatePromises = table.map(async statement => {
				logger.progress({
					message: 'Creating database',
					containerName: statement.DatabaseName,
					entityName: statement.TableInput.Name
				});
				return await glueInstance.createTable(statement).promise();
			});
			await Promise.all(tableCreatePromises);
			callback();
		} catch(err) {
			callback(err);
		}
	},

	async testConnection(connectionInfo, logger, callback, app) {
		setDependencies(app);
		logger.log('info', connectionInfo, 'Test connection', connectionInfo.hiddenKeys);

		const glueInstance = getGlueInstance(connectionInfo, app);

		try {
			await glueInstance.getDatabases().promise();
			callback();
		} catch (err) {
			logger.log('error', { message: err.message, stack: err.stack, error: err }, 'Connection failed');
			callback(err);
		}
	}
};

const buildAWSCLIScript = (containerData, tableSchema) => {
	const dbStatement = getGlueDatabaseCreateStatement(containerData[0]);
	const tableStatement = getGlueTableCreateStatement(tableSchema, containerData[0].name);
	return composeCLIStatements([dbStatement, tableStatement]);
}

const buildAWSCLIModelScript = (containerData, tablesSchemas = {}) => {
	const dbStatement = getGlueDatabaseCreateStatement(containerData[0]);
	const tablesStatements = Object.entries(tablesSchemas).map(([key, value]) => {
		return getGlueTableCreateStatement(value, dependencies.lodash.get(containerData[0], 'name', ''));
	});
	return composeCLIStatements([dbStatement, ...tablesStatements]);
}

const getGlueInstance = (connectionInfo, app) => {
	const { accessKeyId, secretAccessKey, region } = connectionInfo;
	aws.config.update({ accessKeyId, secretAccessKey, region });
	return new aws.Glue();
}

const composeCLIStatements = (statements = []) => {
	return statements.join('\n\n');
}

const buildHiveScript = (needMinify) => (...statements) => {
	const script = statements.filter(statement => statement).join('\n\n');
	if(needMinify) {
		return script;
	}

	return sqlFormatter.format(script, { indent: '    ' });
};

const parseEntities = (entities, serializedItems) => {
	return entities.reduce((result, entityId) => {
		try {
			return Object.assign({}, result, { [entityId]: JSON.parse(serializedItems[entityId]) });
		} catch (e) {
			return result;
		}
	}, {});
};
