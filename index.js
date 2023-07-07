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
  ScanCommand
} from '@aws-sdk/lib-dynamodb'

const ddbClient = new DynamoDBClient({
  region: process.env.AWS_REGION || 'us-east-1',
  secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
  accessKeyId: process.env.AWS_ACCESS_KEY_ID
})

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

class DynamoClient {
  constructor () {
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
    const command = new PutCommand({
      TableName: table,
      Item: obj
    })
    await this.ddbDocClient.send(command)
  }

  async queryInternal (
    table,
    keyConditionExpression,
    expressionAttributeValues
  ) {
    let res = await this.ddbDocClient.send(
      new QueryCommand({
        TableName: table,
        KeyConditionExpression: keyConditionExpression,
        ExpressionAttributeValues: expressionAttributeValues
      })
    )
    let items = res.Items
    while (res.LastEvaluatedKey) {
      res = await this.ddbDocClient.send(
        new QueryCommand({
          TableName: table,
          KeyConditionExpression: keyConditionExpression,
          ExpressionAttributeValues: expressionAttributeValues,
          ExclusiveStartKey: res.LastEvaluatedKey
        })
      )
      items = items.concat(res.Items)
    }
    return items
  }

  async scanInternal (table) {
    let res = await this.ddbDocClient.send(
      new ScanCommand({ TableName: table })
    )
    let items = res.Items
    while (res.LastEvaluatedKey) {
      res = await this.ddbDocClient.send(
        new ScanCommand({
          TableName: table,
          ExclusiveStartKey: res.LastEvaluatedKey
        })
      )
      items = items.concat(res.Items)
    }
    return items
  }

  async query (table, predicate, isSingle) {
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
      items = await this.scanInternal(table)
    } else {
      items = await this.queryInternal(
        table,
        keyConditionExpression,
        expressionAttributeValues
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
      reducedItems.push(item)
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
}

export default new DynamoClient()
