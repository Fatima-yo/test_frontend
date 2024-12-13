const hubspot = require("@hubspot/api-client");
const { queue } = require("async");
const lodash = require("lodash");

const { filterNullValuesFromObject, goal } = require("./utils");
const Domain = require("./Domain");

const hubspotClient = new hubspot.Client({ accessToken: "" });
const propertyPrefix = "hubspot__";
let expirationDate;


const pullDataFromHubspot = async () => {
	console.log("start pulling data from HubSpot");

	const domain = await Domain.findOne({});

	for (const account of domain.integrations.hubspot.accounts) {
		console.log("start processing account");

		try {
			await refreshAccessToken(domain, account.hubId);
		} catch (err) {
			console.log(err, {
				apiKey: domain.apiKey,
				metadata: { operation: "refreshAccessToken" },
			});
		}

		const actions = [];
		const q = createQueue(domain, actions);
		const account_process = domain.integrations.hubspot.accounts.find(
			(account) => account.hubId === account.hubId
		);
		await Promise.all([
			runProccess(
				processMeetings,
				domain,
				account.hubId,
				q,
				account_process,
				"processMeetings"
			),
			runProccess(
				processContacts,
				domain,
				account.hubId,
				q,
				account_process,
				"processContacts"
			),
			runProccess(
				processCompanies,
				domain,
				account.hubId,
				q,
				account_process,
				"processCompanies"
			),
		]);

		try {
			await drainQueue(domain, actions, q);
			console.log("drain queue");
		} catch (err) {
			console.log(err, {
				apiKey: domain.apiKey,
				metadata: { operation: "drainQueue", hubId: account.hubId },
			});
		}

		await saveDomain(domain);

		console.log("finish processing account");
	}

	process.exit();
};

const requestAssociations = async (hubspotClient, from, to, inputs) => {
	try {
		let res =
			(
				await (
					await hubspotClient.apiRequest({
						method: "post",
						path: `/crm/v3/associations/${from}/${to}/batch/read`,
						body: {
							inputs,
						},
					})
				).json()
			)?.results || [];
		return res;
	} catch (error) {
		throw Error("Unable to fetch associations. Error: ", error);
	}
};

const runProccess = async (
	process,
	domain,
	hubId,
	q,
	account_process,
	operation
) => {
	try {
		await process(domain, hubId, q, account_process);
		console.log(`process ${operation}`);
	} catch (err) {
		console.log(err, {
			apiKey: domain.apiKey,
			metadata: { operation, hubId },
		});
	}
};

const generateLastModifiedDateFilter = (
	date,
	nowDate,
	propertyName = "hs_lastmodifieddate"
) => {
	const lastModifiedDateFilter = date
		? {
				filters: [
					{ propertyName, operator: "GTE", value: `${date.valueOf()}` },
					{ propertyName, operator: "LTE", value: `${nowDate.valueOf()}` },
				],
		  }
		: {};

	return lastModifiedDateFilter;
};

const saveDomain = async (domain) => {
	// disable this for testing purposes
	// return;

	domain.markModified("integrations.hubspot.accounts");
	await domain.save();
};

/**
 * Get access token from HubSpot
 */
const refreshAccessToken = async (domain, hubId, tryCount) => {
	const { HUBSPOT_CID, HUBSPOT_CS } = process.env;
	const account = domain.integrations.hubspot.accounts.find(
		(account) => account.hubId === hubId
	);
	const { accessToken, refreshToken } = account;

	return hubspotClient.oauth.tokensApi
		.createToken(
			"refresh_token",
			undefined,
			undefined,
			HUBSPOT_CID,
			HUBSPOT_CS,
			refreshToken
		)
		.then(async (result) => {
			const body = result.body ? result.body : result;

			const newAccessToken = body.accessToken;
			expirationDate = new Date(body.expiresIn * 1000 + new Date().getTime());

			hubspotClient.setAccessToken(newAccessToken);
			if (newAccessToken !== accessToken) {
				account.accessToken = newAccessToken;
			}

			return true;
		});
};

const fetchData = async (
	api,
	searchObject,
	maxRetries = 4,
	domain,
	hubId,
	type
) => {
	let tryCount = 0;
	let result;
	while (tryCount <= maxRetries) {
		try {
			result = await api.doSearch(searchObject);
			break;
		} catch (err) {
			tryCount++;
			console.log(`retry-${tryCount}-${type}`, err);
			if (new Date() > expirationDate) await refreshAccessToken(domain, hubId);
			if (tryCount === maxRetries)
				throw new Error(
					`Failed to fetch data for the ${maxRetries} for ${type}`
				);

			await new Promise((resolve, reject) =>
				setTimeout(resolve, 5000 * Math.pow(2, tryCount))
			);
		}
	}
	return [result, result?.results || []];
};

/**
 * Get recently modified companies as 100 companies per page
 */
const processCompanies = async (domain, hubId, q, account) => {
	const lastPulledDate = new Date(account.lastPulledDates.companies);
	const now = new Date();

	let hasMore = true;
	const offsetObject = {};
	const limit = 100;

	while (hasMore) {
		const lastModifiedDate = offsetObject.lastModifiedDate || lastPulledDate;
		const lastModifiedDateFilter = generateLastModifiedDateFilter(
			lastModifiedDate,
			now
		);
		const searchObject = {
			filterGroups: [lastModifiedDateFilter],
			sorts: [{ propertyName: "hs_lastmodifieddate", direction: "ASCENDING" }],
			properties: [
				"name",
				"domain",
				"country",
				"industry",
				"description",
				"annualrevenue",
				"numberofemployees",
				"hs_lead_status",
			],
			limit,
			after: offsetObject.after,
		};

		let [searchResult, data] = await fetchData(
			hubspotClient.crm.companies.searchApi,
			searchObject,
			4,
			domain,
			hubId,
			"companies"
		);

		offsetObject.after = parseInt(searchResult?.paging?.next?.after);

		console.log("fetch company batch");

		data.forEach((company) => {
			if (!company.properties) return;

			const actionTemplate = {
				includeInAnalytics: 0,
				companyProperties: {
					company_id: company.id,
					company_domain: company.properties.domain,
					company_industry: company.properties.industry,
				},
			};
			addActionQueue(company, "Company", lastPulledDate, q, actionTemplate);
		});

		hasMore = !!offsetObject?.after;

		if (offsetObject?.after >= 9900) {
			offsetObject.after = 0;
			offsetObject.lastModifiedDate = new Date(
				data[data.length - 1].updatedAt
			).valueOf();
		}
	}

	account.lastPulledDates.companies = now;
	await saveDomain(domain);

	return true;
};

/**
 * Get recently modified contacts as 100 contacts per page
 */
const processContacts = async (domain, hubId, q, account) => {
	const lastPulledDate = new Date(account.lastPulledDates.contacts);
	const now = new Date();

	let hasMore = true;
	const offsetObject = {};
	const limit = 100;

	while (hasMore) {
		const lastModifiedDate = offsetObject.lastModifiedDate || lastPulledDate;
		const lastModifiedDateFilter = generateLastModifiedDateFilter(
			lastModifiedDate,
			now,
			"lastmodifieddate"
		);
		const searchObject = {
			filterGroups: [lastModifiedDateFilter],
			sorts: [{ propertyName: "lastmodifieddate", direction: "ASCENDING" }],
			properties: [
				"firstname",
				"lastname",
				"jobtitle",
				"email",
				"hubspotscore",
				"hs_lead_status",
				"hs_analytics_source",
				"hs_latest_source",
			],
			limit,
			after: offsetObject.after,
		};

		let [searchResult, data] = await fetchData(
			hubspotClient.crm.contacts.searchApi,
			searchObject,
			4,
			domain,
			hubId,
			"contacts"
		);

		console.log("fetch contact batch");

		offsetObject.after = parseInt(searchResult.paging?.next?.after);
		const contactIds = data.map((contact) => contact.id);

		const companyAssociationsResults = await requestAssociations(
			hubspotClient,
			"CONTACTS",
			"COMPANIES",
			contactIds.map((contactId) => ({
				id: contactId,
			}))
		);

		const companyAssociations = Object.fromEntries(
			companyAssociationsResults
				.map((a) => {
					if (a.from) {
						contactIds.splice(contactIds.indexOf(a.from.id), 1);
						return [a.from.id, a.to[0].id];
					} else return false;
				})
				.filter((x) => x)
		);

		data.forEach((contact) => {
			if (!contact.properties || !contact.properties.email) return;

			const companyId = companyAssociations[contact.id];

			const userProperties = {
				company_id: companyId,
				contact_name: (
					(contact.properties.firstname || "") +
					" " +
					(contact.properties.lastname || "")
				).trim(),
				contact_title: contact.properties.jobtitle,
				contact_source: contact.properties.hs_analytics_source,
				contact_status: contact.properties.hs_lead_status,
				contact_score: parseInt(contact.properties.hubspotscore) || 0,
			};

			const actionTemplate = {
				includeInAnalytics: 0,
				identity: contact.properties.email,
				userProperties: filterNullValuesFromObject(userProperties),
			};

			addActionQueue(contact, "Contact", lastPulledDate, q, actionTemplate);
		});

		hasMore = !!offsetObject?.after;
		if (offsetObject?.after >= 9900) {
			offsetObject.after = 0;
			offsetObject.lastModifiedDate = new Date(
				data[data.length - 1].updatedAt
			).valueOf();
		}
	}

	account.lastPulledDates.contacts = now;
	await saveDomain(domain);

	return true;
};

/**
 * Get recently modified meetings as 100 meetings per page
 */

const processMeetings = async (domain, hubId, q, account) => {
	const lastPulledDate = new Date(account.lastPulledDates.meetings);
	const now = new Date();

	let hasMore = true;
	const offsetObject = {};
	const limit = 100;

	while (hasMore) {
		const lastModifiedDate = offsetObject.lastModifiedDate || lastPulledDate;
		const lastModifiedDateFilter = generateLastModifiedDateFilter(
			lastModifiedDate,
			now
		);
		const searchObject = {
			filterGroups: [lastModifiedDateFilter],
			sorts: [{ propertyName: "hs_lastmodifieddate", direction: "ASCENDING" }],
			properties: ["title", "meetingDate", "meetingDuration"],
			limit,
			after: offsetObject.after,
		};

		let [searchResult, data] = await fetchData(
			hubspotClient.crm.objects.meetings.searchApi,
			searchObject,
			4,
			domain,
			hubId,
			"meetings"
		);

		console.log("fetch meeting batch");

		offsetObject.after = parseInt(searchResult.paging?.next?.after);
		const meetingsIds = data.map((meeting) => meeting.id);

		const contactAssociationResults = await requestAssociations(
			hubspotClient,
			"MEETINGS",
			"CONTACTS",
			meetingsIds?.map((meetingId) => ({
				id: meetingId,
			}))
		);

		const contactAssociations = Object.fromEntries(
			contactAssociationResults
				.map((a) => {
					if (a.from) {
						meetingsIds.splice(meetingsIds.indexOf(a.from.id), 1);
						return [a.to[0].id, a.from.id];
					} else return false;
				})
				.filter((x) => x)
		);

		let filter = {
			properties: ["email", "name"],
			propertiesWithHistory: [],
			inputs: Object.keys(contactAssociations).map((item) => ({ id: item[0] })),
		};

		let contactResult;
		try {
			contactResult = (await hubspotClient.crm.contacts.batchApi.read(filter))
				.results;
		} catch (error) {
			console.log("Error at fetching contact values for meetings", error);
			throw Error("Error getting contacts data");
		}

		let contactsValues = Object.fromEntries(
			contactResult.map((contact) => {
				let meetingId = contactAssociations[contact.id];
				return [meetingId, { id: contact.id, properties: contact.properties }];
			})
		);

		data.forEach((meeting) => {
			const contact = contactsValues[meeting.id];
			if (
				!meeting.properties ||
				!contact.properties ||
				!contact.properties.email
			)
				return;

			const meetingProperties = {
				contact_id: contact.id,
				contact_name: contact.name,
				meeting_title: meeting.properties.title,
				meeting_date: meeting.properties.meetingDate,
				meeting_duration: meeting.properties.meetingDuration,
			};

			const actionTemplate = {
				includeInAnalytics: 0,
				contact_email: contact.email,
				meetingProperties: filterNullValuesFromObject(meetingProperties),
			};

			addActionQueue(meeting, "Meeting", lastPulledDate, q, actionTemplate);
		});

		hasMore = !!offsetObject?.after;
		if (offsetObject?.after >= 9900) {
			offsetObject.after = 0;
			offsetObject.lastModifiedDate = new Date(
				data[data.length - 1].updatedAt
			).valueOf();
		}
	}

	account.lastPulledDates.meetings = now;
	await saveDomain(domain);

	return true;
};

const createQueue = (domain, actions) =>
	queue(async (action, callback) => {
		actions.push(action);

		if (actions.length > 2000) {
			console.log("inserting actions to database", {
				apiKey: domain.apiKey,
				count: actions.length,
			});

			const copyOfActions = lodash.cloneDeep(actions);
			actions.splice(0, actions.length);

			goal(copyOfActions);
		}

		callback();
	}, 100000000);

const addActionQueue = (item, type, lastPulledDate, q, actionTemplate) => {
	const isCreated = new Date(meeting.createdAt) > lastPulledDate;

	q.push({
		actionName: isCreated ? `${type} Created` : `${type} Updated`,
		actionDate: new Date(isCreated ? item.createdAt : item.updatedAt) - 2000,
		...actionTemplate,
	});
};

const drainQueue = async (domain, actions, q) => {
	if (q.length() > 0) await q.drain();

	if (actions.length > 0) {
		goal(actions);
	}

	return true;
};

module.exports = pullDataFromHubspot;
