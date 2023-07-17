import {
  DynamoDBClient,
  ListTablesCommand,
  DescribeTableCommand
} from '@aws-sdk/client-dynamodb'
import {
  DynamoDBDocumentClient,
  QueryCommand,
  PutCommand,
  UpdateCommand,
  ScanCommand,
  DeleteCommand
} from '@aws-sdk/lib-dynamodb'

class DynamoClient {
  constructor (ddbDocClient) {
    this.ddbDocClient = ddbDocClient
    this.isInited = false
    this.tableSchemas = {}
  }

  async init () {
    if (this.isInited) {
      return
    }
    const res = await this.ddbDocClient.send(new ListTablesCommand({}))
    for (const table of res.TableNames) {
      this.tableSchemas[table] = {}
      const res2 = await this.ddbDocClient.send(
        new DescribeTableCommand({ TableName: table })
      )
      for (const key of res2.Table.KeySchema) {
        if (key.KeyType === 'HASH') {
          this.tableSchemas[table].partitionKey = key.AttributeName
        } else if (key.KeyType === 'RANGE') {
          this.tableSchemas[table].sortKey = key.AttributeName
        }
      }
    }
    this.isInited = true
  }

  async insert (table, obj) {
    await this.init()
    const item = {}
    for (const key in obj) {
      if (obj[key]) {
        item[key] = obj[key].toString()
      } else {
        item[key] = null
      }
    }
    const commandObj = {
      TableName: table,
      Item: obj
    }
    // throw an error if an item with the partition key already exists
    if (this.tableSchemas[table].partitionKey && !this.tableSchemas[table].sortKey) {
      commandObj.ConditionExpression = 'attribute_not_exists(' + this.tableSchemas[table].partitionKey + ')'
    }
    const command = new PutCommand(commandObj)
    await this.ddbDocClient.send(command)
  }

  async #queryInternal (
    table,
    keyConditionExpression,
    expressionAttributeValues,
    attributesToGet
  ) {
    let commandObj = {
      TableName: table,
      KeyConditionExpression: keyConditionExpression,
      ExpressionAttributeValues: expressionAttributeValues
    }
    if (attributesToGet) {
      commandObj.AttributesToGet = attributesToGet
    }
    let res = await this.ddbDocClient.send(
      new QueryCommand(commandObj)
    )
    let items = res.Items
    while (res.LastEvaluatedKey) {
      commandObj = {
        TableName: table,
        KeyConditionExpression: keyConditionExpression,
        ExpressionAttributeValues: expressionAttributeValues,
        ExclusiveStartKey: res.LastEvaluatedKey
      }
      if (attributesToGet) {
        commandObj.AttributesToGet = attributesToGet
      }
      res = await this.ddbDocClient.send(
        new QueryCommand(commandObj)
      )
      items = items.concat(res.Items)
    }
    return items
  }

  async #scanInternal (table) {
    let commandObj = {
      TableName: table
    }
    let res = await this.ddbDocClient.send(
      new ScanCommand(commandObj)
    )
    let items = res.Items
    while (res.LastEvaluatedKey) {
      commandObj = {
        TableName: table,
        ExclusiveStartKey: res.LastEvaluatedKey
      }
      res = await this.ddbDocClient.send(
        new ScanCommand(commandObj)
      )
      items = items.concat(res.Items)
    }
    return items
  }

  async query (table, predicate, isSingle, attributesToGet) {
    await this.init()
    const keyConditionExpressionParts = []
    const expressionAttributeValues = {}
    let useScan = true
    for (const key in predicate) {
      if (key === this.tableSchemas[table].partitionKey) {
        keyConditionExpressionParts.push(key + ' = :' + key)
        expressionAttributeValues[':' + key] = predicate[key].toString()
        useScan = false
      }
      if (key === this.tableSchemas[table].sortKey) {
        keyConditionExpressionParts.push(key + ' = :' + key)
        expressionAttributeValues[':' + key] = predicate[key].toString()
      }
    }
    const keyConditionExpression = keyConditionExpressionParts.join(' AND ')

    let items = []
    if (useScan) {
      items = await this.#scanInternal(table)
    } else {
      items = await this.#queryInternal(
        table,
        keyConditionExpression,
        expressionAttributeValues,
        attributesToGet
      )
    }
    if (items.length === 0 && isSingle) {
      return null
    }

    const reducedItems = []
    for (const item of items) {
      if (useScan) {
        let allMatch = true
        for (const key in predicate) {
          if (item[key] !== predicate[key]) {
            allMatch = false
            break
          }
        }
        if (!allMatch) {
          continue
        }
      }
      if (attributesToGet) {
        const reducedItem = {}
        for (const key of attributesToGet) {
          if (item[key] !== undefined) {
            reducedItem[key] = item[key]
          }
        }
        reducedItems.push(reducedItem)
      } else {
        reducedItems.push(item)
      }
    }
    if (isSingle) {
      return reducedItems[0]
    }
    return reducedItems
  }

  async update (table, predicate, obj) {
    await this.init()

    const keyDict = {}
    const expressionAttributeValues = {}
    for (const key in predicate) {
      if (
        key === this.tableSchemas[table].partitionKey ||
        key === this.tableSchemas[table].sortKey
      ) {
        keyDict[key] = predicate[key].toString()
      }
    }
    const setExpressionParts = []
    const removeExpressionParts = []
    for (const key in obj) {
      if (obj[key] === null || obj[key] === undefined) {
        removeExpressionParts.push(key)
      } else {
        setExpressionParts.push(key + ' = :' + key)
        expressionAttributeValues[':' + key] = obj[key]
      }
    }
    let updateExpression = ''
    if (setExpressionParts.length > 0) {
      updateExpression += 'SET ' + setExpressionParts.join(', ')
    }
    if (removeExpressionParts.length > 0) {
      updateExpression += ' '
      updateExpression += 'REMOVE ' + removeExpressionParts.join(', ')
    }
    const command = new UpdateCommand({
      TableName: table,
      Key: keyDict,
      UpdateExpression: updateExpression,
      ExpressionAttributeValues: expressionAttributeValues
    })
    await this.ddbDocClient.send(command)
  }

  async delete (table, predicate) {
    await this.init()

    const keyDict = {}
    for (const key in predicate) {
      if (
        key === this.tableSchemas[table].partitionKey ||
        key === this.tableSchemas[table].sortKey
      ) {
        keyDict[key] = predicate[key].toString()
      }
    }
    const command = new DeleteCommand({
      TableName: table,
      Key: keyDict
    })
    await this.ddbDocClient.send(command)
  }
}

export const connect = ({ region, secretAccessKey, accessKeyId }) => {
  const ddbClient = new DynamoDBClient({ region, secretAccessKey, accessKeyId })

  const marshallOptions = {
    convertEmptyValues: false,
    removeUndefinedValues: true,
    convertClassInstanceToMap: false
  }

  const unmarshallOptions = {
    wrapNumbers: false
  }

  const ddbDocClient = DynamoDBDocumentClient.from(ddbClient, {
    marshallOptions,
    unmarshallOptions
  })
  return new DynamoClient(ddbDocClient)
}
