const { CLI, CREATE_DATABASE } = require('./cliConstants');
const _ = require('lodash');

const getGlueDatabaseCreateStatement = (containerData) => {
	const dbParameters = {
		DatabaseInput: {
			Name: _.get(containerData, 'name', ''),
			Description: _.get(containerData, 'description', ''),
		}
	}

	const cliStatement = `${CLI} ${CREATE_DATABASE} '${JSON.stringify(dbParameters, null, 2)}'`;
	return cliStatement;
}

module.exports = {
	getGlueDatabaseCreateStatement
};