from py2neo import Graph, NodeMatcher, Node, Relationship, RelationshipMatcher
from py2neo.matching import *

# TODO 增
## TODO 增加关系
def addRelationShip(label1, label2, ID) :
    # TODO 创建关系(已有节点)
    matchList1 = list(matcher.match(label1, id = ID))
    matchList2 = list(matcher.match(label2, id = ID))
    # print(len(matchList1))
    # print(matchList1)
    # print(matchList2)
    # print(matchList2[1]['discount'])
    # TODO  author ---> (own) --> name
    # TODO  name ---> (has) --> discount
    # TODO  name ---> (belongto) --> type

    rLabel1 = matchList1[0]['id'] + "_own"
    # print(rLabel1)
    fromNode1 = Node(rLabel1, author = matchList1[0]['author'])
    toNode1 = Node(rLabel1, name = matchList1[0]['name'])
    own_relation = Relationship(fromNode1, "own", toNode1)

    rLabel2 = matchList1[0]['id'] + "_has"
    # print(rLabel2)
    fromNode2 = Node(rLabel2, name=matchList1[0]['name'])
    if label2.__contains__('insert') :
        toNode2 = Node(rLabel2, discount=matchList2[1]['discount'])
    else :
        toNode2 = Node(rLabel2, discount=matchList2[0]['discount'])
    has_relation = Relationship(fromNode2, "has", toNode2)

    rLabel3 = matchList1[0]['id'] + "_belongto"
    # print(rLabel3)
    fromNode3 = Node(rLabel3, name=matchList1[0]['name'])
    toNode3 = Node(rLabel3, type=matchList1[0]['type'])
    belongto_relation = Relationship(fromNode3, "belongto", toNode3)

    # 将创建关系传递到图上
    total_rs = own_relation | has_relation | belongto_relation
    graph.create(total_rs)

    print("Add relation{own,has,belongto} successfully!")


## TODO 增加节点
def addNode(arr) :
    # TODO 创建节点
    node1 = Node(arr[10], id = arr[0], type = arr[1], name = arr[2],
                author = arr[3], price = arr[4])
    node2 = Node(arr[10], id = arr[0], discount=arr[5],pub_time=arr[6], pricing=arr[7],
                 publisher=arr[8],crawler_time=arr[9])
    graph.create(node1)
    graph.create(node2)
    print('Add node successfully!')

# TODO 删
# 1. 删除全部结点
def deleteAllNode() :
    graph.delete_all()
    print('删除成功！')

# 2. 根据ID删除一个节点(不能删除含有关系的节点)
def deleteOneNodeById(label, fieldValue) :
    node = matcher.match(label).where(id = fieldValue).first()  # 先匹配，叫fieldValue的第一个结点
    graph.delete(node)
    print('标签为' + label + ', id为' + fieldValue + '的一个节点成功删除！')

# 3. 删除关系
def deleteRelationship(label, rsName) :
    # rs = matcher.match(r_type = rsName).first()
    # graph.delete(rs)

    # node = matcher.match(label, r_type=rsName)
    # graph.separate(node)
    # TODO match(n:`2_has`)-[r:has]-() delete r
    deleteCQL1 = 'match(n:' + "`" + label + "`" + ')-[r:' + rsName + ']-() delete r'
    print(deleteCQL1)
    try :
        graph.run(deleteCQL1)
        print('delete label[' + label + '],' + 'relationship[' +  rsName + '] successufully!')
    except Exception:
        print('Happen Exception')



# TODO 改
def update(label, fieldName, fieldValue, newValue) :
    # 1.改变属性
    node = matcher.match(label).where(fieldName = fieldValue).first() # 评估匹配并返回匹配的第一个节点
    # setdefault() 如果此节点具有的属性age，则返回其值。如果没有，添加‘age’属性和对应的属性值'30'
    node['number'] = '001'    #添加“number”属性对应的属性值为“002”
    node['age'] = '10'
    node['home'] = 'AnHui'
    print('node:')
    print(node) #打印节点信息
    graph.push(node)    #将更改或者添加的放到图中

    # 2.改变和查同步
    node = Node(label, fieldName = newValue)
    graph.create(node)
    node_id = node.identity
    print('修改之前：', node)
    print('id号是：', node_id)
    create_node = matcher[node_id]
    node['name'] = 'cat'
    print('修改之后:', node)

def updateFieldVal(label, ID, fieldName, newValue):
    # TODO 修改属性值
    # node = matcher.match(label, id=ID)
    # field = "'" + fieldName + "'"
    # val = "'" + newValue + "'"
    # data = {
    #     # field : val
    #     'name' : 'uname'
    # }
    # node.update(data)
    # graph.push(node)
    # print('update sucessfully!')
    #
    # matchNode = Node(label, id=ID)
    # matchNode.update(data)
    # graph.push(matchNode)
    # print('update sucessfully!')

    uCQL = 'match(b:' + label +  ') where b.id = ' + "\'" + ID + "\'"  + ' set b.' + fieldName + '=' + \
           "\'" + newValue + "\'" + ' return b'
    print(uCQL)
    try :
        graph.run(uCQL)
        print('update sucessfully!')
    except Exception :
        print('Happen Exception!')


# TODO 查
# 1. 查找节点id/根据id查找节点
def queryById(label, ID) :
    print('所有的节点类型为：', graph.schema.node_labels)  # 查看节点类型
    res = list(matcher.match(label, id = ID))
    print(res)

def querySortingByField(label, fieldName) :
    pre = list(matcher.match(label))
    # print(pre)
    if pre.__eq__(list()):
        print('The label what you query is not exists!')
    else :
        res = querySortingByFieldHelper(label, fieldName)
        if res is not None :
            print(res)
        # res = str(res)
        # if res != 'None':
        #     print(res)


def querySortingByFieldHelper(label, fieldName) :
    flag = True
    if fieldName.__eq__('id'):
        res = list(matcher.match(label).order_by('_.id').limit(5))
    elif fieldName.__eq__('type'):
        res = list(matcher.match(label).order_by('_.type').limit(5))
    elif fieldName.__eq__('name'):
        res = list(matcher.match(label).order_by('_.name').limit(5))
    elif fieldName.__eq__('author'):
        res = list(matcher.match(label).order_by('_.author').limit(5))
    elif fieldName.__eq__('price'):
        res = list(matcher.match(label).order_by('_.price').limit(5))
    elif fieldName.__eq__('discount'):
        res = list(matcher.match(label).order_by('_.discount').limit(5))
    elif fieldName.__eq__('pub_time'):
        res = list(matcher.match(label).order_by('_.pub_time').limit(5))
    elif fieldName.__eq__('pricing'):
        res = list(matcher.match(label).order_by('_.pricing').limit(5))
    elif fieldName.__eq__('publisher'):
        res = list(matcher.match(label).order_by('_.publisher').limit(5))
    elif fieldName.__eq__('crawler_time'):
        res = list(matcher.match(label).order_by('_.crawler_time').limit(5))
    else:
        flag = False
    if flag :
        return res
    else :
        print('不存在这个属性！')

# TODO 查关系，找到所有关系
def queryRelationship(rsName) :
    all_r_types = list(graph.schema.relationship_types)
    # print('所有的关系类型有：', all_r_types)  # 查看关系的类型

    # node = matcher.match(label)
    # relationship_matcher = RelationshipMatcher(graph)
    # res = relationship_matcher.match([node], r_type = rsName, label = label)
    # print(res)

    all = list(graph.match(nodes=None, r_type=None, limit=None))
    # print('所有的关系为；')
    # print(all)
    # for i in all :
    #     print(i)

    flag = True
    if rsName.__eq__('own') :
        print('你所查询的关系【' + rsName + '】结果为：')
        one = list(graph.match(nodes=None, r_type = 'own'))
    elif rsName.__eq__('has') :
        print('你所查询的关系【' + rsName + '】结果为：')
        one = list(graph.match(nodes=None, r_type = 'has'))
    elif rsName.__eq__('belongto') :
        print('你所查询的关系【' + rsName + '】结果为：')
        one = list(graph.match(nodes=None, r_type = 'belongto'))
    else :
        flag = False
        print('你所查询的关系【' + rsName + '】不存在！')

    if flag :
        print(one)

    # # print(res.__iter__())
    # iterator = RelationshipMatcher.__iter__(res)
    # while True :
    #     try :
    #         ele = RelationshipMatcher.__iter__(res).__next__()
    #         print(ele)
    #     except Exception :
    #         print('Happen Exception')
    #         break


def menu() :
    print("|----------------------- Neo4j增删改查操作 ---------------------------------|")
    print("|----------------------- 1. 根据ID查找节点  --------------------------------|")
    print("|----------------------- 2. 按照属性排序查找Top5节点 ------------------------|")
    print("|----------------------- 3. 查找关系  -------------------------------------|")
    print("|----------------------- 4. 新增节点 --------------------------------------|")
    print("|----------------------- 5. 为节点增加关系 ---------------------------------|")
    print("|----------------------- 6. 修改指定属性值 ---------------------------------|")
    print("|----------------------- 7. 根据ID删除一个节点 -----------------------------|")
    print("|----------------------- 8. 删除指定关系 -----------------------------------|")
    # print("|----------------------- 9. 删除全部节点 ---------------------------------|")
    print("|----------------------- 0. 退出系统 --------------------------------------|")
    print("|-------------------------------------------------------------------------|")


if __name__ == '__main__':
    # 连接Neo4j数据库输入地址、用户名、密码
    url = 'bolt://10.125.0.15:7687'
    usr = 'neo4j'
    key = '123456'
    graph = Graph(url, auth=(usr, key))
    matcher = NodeMatcher(graph)  # 创建节点匹配器

    while (True) :
        menu()
        option = input("please input your choice:")
        option = int (option)

        if (option == 1) :
            print("------------------ 根据ID查找节点 --------------------")
            inputQArrById = input('input the label, ID what want to query:')
            qArrById = inputQArrById.split(" ")
            queryById(qArrById[0], qArrById[1])
            # print('------------- 节点个数：', len(graph.nodes))
        elif (option == 2) :
            print("------------------ 按照属性排序查找Top5节点 --------------------")
            inputFQArr = input('input the label, field what want to query:')
            qSFArr = inputFQArr.split(" ")
            querySortingByField(qSFArr[0], qSFArr[1])
        elif (option == 3) :
            print("-------------------- 查找指定关系 --------------------")
            inputINArr = input('input the relationship what want to add:')
            # qRSArr = inputINArr.split(" ")
            queryRelationship(inputINArr)
        elif (option == 4) :
            print("-------------------- 增加节点 --------------------")
            inputINArr = input('input the data what want to add:')
            iNArr = inputINArr.split(" ")
            addNode(iNArr)
        elif (option == 5) :
            print("------------------ 为节点增加关系 --------------------")
            inputARArr = input('input the labels, id what want to add:')
            idArr = inputARArr.split(" ")
            addRelationShip(idArr[0], idArr[1], idArr[2])
        elif (option == 6) :
            print("-------------------- 修改指定属性值 ------------------")
            inputUFArr = input('input the label, id, field, newValue what want to update:')
            uArr = inputUFArr.split(" ")
            updateFieldVal(uArr[0], uArr[1], uArr[2], uArr[3])
        elif (option == 7) :
            print("-------------------- 根据ID删除一个节点 ---------------------")
            inputDArr = input('input the label, ID what want to delete:')
            dArr = inputDArr.split(" ")
            deleteOneNodeById(dArr[0], dArr[1])
        elif (option == 8) :
            print("-------------------- 删除指定关系 ----------------------")
            inputDRArr = input('input the label, relationship what want to delete:')
            dArr = inputDRArr.split(" ")
            deleteRelationship(dArr[0], dArr[1])
        # elif (option == 9):
        #     print("-------------------- 删除全部节点 ----------------------")
        #     deleteAllNode()
        elif (option == 0) :
            print("Thanks you for using the system!")
            exit(0)
            break
        else :
            print("the choice what you input is error!")

