/**
 * Created by yuanjianxin on 2018/7/3.
 */
const grpc=require('grpc');
const messages=require('grpc-db-service-pb').DbService_pb;
const services=require('grpc-db-service-pb').DbService_grpc_pb;

module.exports=class _handler{
    static get instance(){
        if(!_handler._instance)
            _handler._instance=new _handler();
        return _handler._instance;
    }

    constructor(){
        this.host=null;
        this.port=null;
        this.dataSource=null;
        this.pool=[];
        this.poolSize=5;//连接池

        // alias
        this.findById = this.get;
        this.findOne = this.getOne;
        this.findAll = this.list;
        this.create = this.save;
    }

    config({host,port,dataSource,poolSize}){
        this.host=host;
        this.port=port;
        this.dataSource=dataSource;
        poolSize && (this.poolSize=poolSize);
        this.initClients();
    }

    initClients(){
        Array(this.poolSize).fill(null).forEach(v=>{
            this.pool.push(new services.DbServiceClient(`${this.host}:${this.port}`,grpc.credentials.createInsecure()));
        })
    }

    getClient(){
        return this.pool.shift() || new services.DbServiceClient(`${this.host}:${this.port}`,grpc.credentials.createInsecure());
    }

    get(table,paras,dataSource=null){
        let request=new messages.getRequest();
        dataSource=dataSource || this.dataSource;
        request.setDatasource(JSON.stringify(dataSource));
        request.setTable(JSON.stringify(table));
        request.setParas(JSON.stringify(paras));
        let client = this.getClient();
        return new Promise((resolve,reject)=>{
            client.get(request,(err,data)=>{
                this.pool.length<this.poolSize  && this.pool.push(client) || client.close();
                err ? reject(err) : resolve(JSON.parse(data.getResult()));
            })
        });
    }

    getOne(table,where,paras,dataSource=null){
        dataSource=dataSource || this.dataSource;
        let request=new messages.getOneRequest();
        request.setDatasource(JSON.stringify(dataSource));
        request.setTable(JSON.stringify(table));
        request.setWhere(JSON.stringify(where));
        request.setParas(JSON.stringify(paras));
        let client=this.getClient();
        return new Promise((resolve,reject)=>{
            client.getOne(request,(err,data)=>{
                this.pool.length<this.poolSize  && this.pool.push(client) || client.close();
                err ? reject(err) : resolve(JSON.parse(data.getResult()));
            })
        });
    }

    list(table,where,dataSource=null){
        dataSource=dataSource || this.dataSource;
        let request=new messages.listRequest();
        request.setDatasource(JSON.stringify(dataSource));
        request.setTable(JSON.stringify(table));
        request.setWhere(JSON.stringify(where));
        let client=this.getClient();
        return new Promise((resolve,reject)=>{
            client.list(request,(err,data)=>{
                this.pool.length<this.poolSize  && this.pool.push(client) || client.close();
                err ? reject(err) : resolve(JSON.parse(data.getResult()));
            })
        });
    }

    save(table,paras,where='id',dataSource=null){
        dataSource=dataSource || this.dataSource;
        let request=new messages.saveRequest();
        request.setDatasource(JSON.stringify(dataSource));
        request.setTable(JSON.stringify(table));
        request.setWhere(JSON.stringify(where));
        request.setParas(JSON.stringify(paras));
        let client=this.getClient();
        return new Promise((resolve,reject)=>{
            client.save(request,(err,data)=>{
                this.pool.length<this.poolSize  && this.pool.push(client) || client.close();
                err ? reject(err) : resolve(JSON.parse(data.getResult()));
            })
        });
    }

    update(table,where,paras,dataSource=null){
        dataSource=dataSource || this.dataSource;
        let request=new messages.updateRequest();
        request.setDatasource(JSON.stringify(dataSource));
        request.setTable(JSON.stringify(table));
        request.setWhere(JSON.stringify(where));
        request.setParas(JSON.stringify(paras));
        let client=this.getClient();
        return new Promise((resolve,reject)=>{
            client.update(request,(err,data)=>{
                this.pool.length<this.poolSize  && this.pool.push(client) || client.close();
                err ? reject(err) : resolve(data.getResult());
            })
        });
    }

    del(table,id,dataSource=null){
        dataSource=dataSource || this.dataSource;
        let request=new messages.delRequest();
        request.setDatasource(JSON.stringify(dataSource));
        request.setTable(JSON.stringify(table));
        request.setId(JSON.stringify(id));
        let client=this.getClient();
        return new Promise((resolve,reject)=>{
            client.del(request,(err,data)=>{
                this.pool.length<this.poolSize  && this.pool.push(client) || client.close();
                err ? reject(err) : resolve(data.getResult());
            })
        });
    }

    multiGet(table,id,field="id",dataSource=null){
        dataSource=dataSource || this.dataSource;
        let request=new messages.multiGetRequest();
        request.setDatasource(JSON.stringify(dataSource));
        request.setTable(JSON.stringify(table));
        request.setId(JSON.stringify(id));
        request.setField(JSON.stringify(field));
        let client=this.getClient();
        return new Promise((resolve,reject)=>{
            client.multiGet(request,(err,data)=>{
                this.pool.length<this.poolSize  && this.pool.push(client) || client.close();
                err ? reject(err) : resolve(JSON.parse(data.getResult()));
            })
        });
    }

    toOne(table,where,paras,result,dataSource=null){
        dataSource=dataSource || this.dataSource;
        let request=new messages.toOneRequest();
        request.setDatasource(JSON.stringify(dataSource));
        request.setTable(JSON.stringify(table));
        request.setWhere(JSON.stringify(where));
        request.setParas(JSON.stringify(paras));
        request.setResult(JSON.stringify(result));
        let client=this.getClient();
        return new Promise((resolve,reject)=>{
            client.toOne(request,(err,data)=>{
                this.pool.length<this.poolSize  && this.pool.push(client) || client.close();
                err ? reject(err) : resolve(JSON.parse(data.getResult()));
            })
        });
    }

    toMany(table,where,paras,result,dataSource=null){
        dataSource=dataSource || this.dataSource;
        let request=new messages.toManyRequest();
        request.setDatasource(JSON.stringify(dataSource));
        request.setTable(JSON.stringify(table));
        request.setWhere(JSON.stringify(where));
        request.setParas(JSON.stringify(paras));
        request.setResult(JSON.stringify(result));
        let client=this.getClient();
        return new Promise((resolve,reject)=>{
            client.toMany(request,(err,data)=>{
                this.pool.length<this.poolSize  && this.pool.push(client) || client.close();
                err ? reject(err) : resolve(JSON.parse(data.getResult()));
            })
        });
    }

    count(table,where,dataSource=null){
        dataSource=dataSource || this.dataSource;
        let request=new messages.countRequest();
        request.setDatasource(JSON.stringify(dataSource));
        request.setTable(JSON.stringify(table));
        request.setWhere(JSON.stringify(where));
        let client=this.getClient();
        return new Promise((resolve,reject)=>{
            client.count(request,(err,data)=>{
                this.pool.length<this.poolSize  && this.pool.push(client) || client.close();
                err ? reject(err) : resolve(JSON.parse(data.getResult()));
            })
        });
    }

    sum(table,field,where,dataSource=null){
        dataSource=dataSource || this.dataSource;
        let request=new messages.sumRequest();
        request.setDatasource(JSON.stringify(dataSource));
        request.setTable(JSON.stringify(table));
        request.setWhere(JSON.stringify(where));
        request.setField(JSON.stringify(field));
        let client=this.getClient();
        return new Promise((resolve,reject)=>{
            client.sum(request,(err,data)=>{
                this.pool.length<this.poolSize  && this.pool.push(client) || client.close();
                err ? reject(err) : resolve(JSON.parse(data.getResult()));
            })
        });
    }





};