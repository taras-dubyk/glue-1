const { getTypeByProperty, getUnionFromAllOf, getUnionFromOneOf } = require('../columnHelper');

const getGlueTableColumns = (properties = {}, oneOf, allOf) => {
	const unionColumns = getUnionColumns(allOf, oneOf);
	const columns = Object.entries(properties)
		.filter(([key, value]) => !value.compositePartitionKey)
		.map(([key, value]) => mapColumn(key, value));
	return [...unionColumns, ...columns];
};

const getGluePartitionKeyTableColumns = (properties = {}) => {
	return Object.entries(properties)
		.filter(([key, value]) => value.compositePartitionKey)
		.map(([key, value]) => mapColumn(key, value));
};

const getGlueTableClusteringKeyColumns = (properties = {}) => {
	return Object.entries(properties)
		.filter(([key, value]) => value.compositeClusteringKey)
		.map(([key, value]) => key);
};

const getGlueTableSortingColumns = (sortingItems = [], properties = {}) => {
	return sortingItems.map(item => {
		const property = Object.entries(properties).find(([key, value]) => value.id === item.id);
		const propertyName = property && property[0];
		return {
			Column: propertyName,
			SortOrder: item.type === 'ascending' ? 1 : 0
		};
	});
};

const mapColumn = (name, data) => {
	return {
		Name: name,
		Type: getTypeByProperty(data),
		Comment: data.comments
	};
}

const getUnionColumns = (allOf, oneOf) => {
	let columns = [];

	if (Array.isArray(oneOf)) {
		const unions = getUnionFromOneOf(getTypeByProperty)({ oneOf });
		const oneOfColumns = Object.keys(unions).reduce((acc, typeName) => {
			acc = [...acc, { Name: typeName, Type: unions[typeName] }]
			return acc;
		}, []);
		columns = [...columns, ...oneOfColumns];
	} 
	
	if (Array.isArray(allOf)) {
		const unions = getUnionFromAllOf(getTypeByProperty)({ allOf });
		
		const allOfColumns = Object.keys(unions).reduce((acc, typeName) => {
			acc = [...acc, { Name: typeName, Type: unions[typeName] }]
			return acc;
		}, []);
		columns = [...columns, ...allOfColumns];
	}

	return columns;
}

module.exports = {
	getGlueTableColumns,
	getGluePartitionKeyTableColumns,
	getGlueTableClusteringKeyColumns,
	getGlueTableSortingColumns
};
