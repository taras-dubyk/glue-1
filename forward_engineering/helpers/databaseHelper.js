'use strict'

const { buildStatement, getName, getTab, replaceSpaceWithUnderscore } = require('./generalHelper');

const getCreateStatement = ({
	name, comment, location, dbProperties
}) => buildStatement(`CREATE DATABASE IF NOT EXISTS ${name}`)
	(comment, `COMMENT "${comment}"`)
	(location, `LOCATION "${location}"`)
	(dbProperties, `WITH DBPROPERTIES (${dbProperties})`)
	() + ';';

const getDatabaseStatement = (containerData) => {
	const tab = getTab(0, containerData);
	
	return getCreateStatement({
		name: replaceSpaceWithUnderscore(getName(tab)),
		comment: tab.comments
	});
};

module.exports = {
	getDatabaseStatement
};
