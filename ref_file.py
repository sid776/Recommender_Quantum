import os
import ast
import json
import pandas as pd
import datetime
import logging
import hashlib
from ninja import Router
from django.shortcuts import render
from django.http import JsonResponse
from objects.mktdata.riskfactor import RiskFactor
from objects.portfolio.products import Products
from objects.portfolio.entities import Entities
from objects.portfolio.hierarchy import BookHierarchy, ProductHierarchy
from objects.var.var_results import VaRResults
from django.contrib.auth.decorators import login_required
from objects.var.model import VaRModel, VaRModelRiskfactor, VaRModelProduct, VaRModelEntity
from services.api import frontend_schema
from typing import Union
from core.storage import AzStorage
from django.views.decorators.http import require_GET
from objects.workflow.request import Requests
from objects.workflow.requestrun import RequestRun
from services.api.utils import get_sort_key
from django.http import HttpResponse
from util.caching.caches import rcache
from util.timer import Timer

logger = logging.getLogger(__name__)
router = Router(tags=["APIs exposed to frontend"])

def fix_and_parse(s):
    try:
        return json.loads(s.replace("'", '"'))
    except json.JSONDecodeError:
        return {}
    
@login_required
@require_GET
def frontend_app_view(request):
    context = {
            "agGridLicenseKey": os.getenv("AG_GRID_LICENSE"),
            "user_info": {
                "user_name": request.user.username,
                "first_name": request.user.first_name,
                "last_name": request.user.last_name,
                "email": request.user.email
            }
        }
    return render(
        request, "base.html", context
    )

@router.get('/riskfactor/all', response=Union[frontend_schema.RiskFactorDetailListDTO, frontend_schema.RiskFactorsListDTO])
def get_riskfactors(request, cob_date=None, details=True):
    with Timer("Trigger riskfactor/all AAPI") as timer:
        if details:
            if not cob_date:
                from core.db import DBConnection
                with DBConnection() as db:
                    schema = db.layer_map['silver']
                    query = f"select max(COB_DT) from {schema}.futuresexpirymapping"
                    cob_date = str(db.execute(query, df=True).values[0][0])
            data = RiskFactor.get_risk_factor_detail_list(cob_date)
            # return JsonResponse({"details": data})
            return dict(details=data, cob_date=cob_date)
    return dict(riskfactors=RiskFactor.get_risk_factor_list())

@router.get('/riskfactor/byname', response=frontend_schema.RiskFactorDTO)
def get_riskfactor_by_name(request, name: str):
    with Timer("Triggered riskfactor/byname API call") as timer:
        obj = RiskFactor.get(risk_factor_name=name)

    return obj.to_dict()

@router.post('/riskfactor/save')
def save_riskfactor(request):    
    return JsonResponse({"message": "Update Risk Factor"})

@router.get('/varmodel/all')
def get_var_model_names(request): 
    with Timer("Triggered varmodel/all api") as timer:
        results = dict(models=VaRModel.get_var_model_list())

    return results

@router.get('/varmodel/byname')
def get_var_model_by_name(request, name):  
    with Timer("Triggered varmodel/byname api") as timer:  
        obj = VaRModel.get(model_name=name)
        response = obj.to_dict()
        response["confidence_level"] = int(round(response["confidence_level"] * 100))

    return response

@router.post('/varmodel/save')
def save_var_model(request):
    with Timer("Triggered varmodel/save api") as timer:
        response = json.loads(request.body)
        is_new_varmodel = response['var_model']['isNew']
        varmodel_id = response["var_model"].get("value")
        var_model = response['var_model'].get("label")
        var_type = response["var_type"].get("value") if isinstance(response["var_type"], dict) else response["var_type"]
        confidence_level = int(response["confidence_level"])/100
        holding_period = int(response["holding_period"])
        time_horizon_period = int(response["time_horizon_period"])
        status_cd = response["status_cd"].get("value") if isinstance(response["status_cd"], dict) else response["status_cd"]
        pnl_identifier = hashlib.md5(f"{var_type},{holding_period}".encode()).hexdigest()[:8]
        current_timestamp = datetime.datetime.now().timestamp()
        if is_new_varmodel:
            with Timer(f"Inseting new varmodel {var_model}") as insert_timer:
                varmodel_id = VaRModel.get_max_id() + 1
                VaRModel.insert(
                    model_id=varmodel_id, model_name=var_model,
                    var_type=var_type, confidence_level=confidence_level,
                    time_horizon=time_horizon_period, holding_period=holding_period,
                    status_code=status_cd, is_official=True, pnl_identifier=pnl_identifier,
                    created_by=request.user.username, valid_from=current_timestamp,
                    valid_to='2200-12-31T05:00'
                )
        else:
            with Timer(f"Updating varmodel id {varmodel_id}") as update_timer:
                VaRModel.update(set_cols={"var_type": var_type, "confidence_level": confidence_level,
                                    "time_horizon": time_horizon_period, "holding_period": holding_period,
                                    "status_code": status_cd}, model_id=varmodel_id)

        #Handle RiskFactors
        if response['risk_factors'].get('deleted'):
            with Timer(f"Updating riskfactors = {varmodel_id}") as riskfactor_timer:
                deleted_df = VaRModelRiskfactor.get_dataframe(model_id=varmodel_id, riskfactor_id=response['risk_factors'].get('deleted'), valid_to = '2200-12-31T05:00', pyspark=False)
                if len(deleted_df):
                    VaRModelRiskfactor.update(set_cols=dict(valid_to=current_timestamp), id=deleted_df.id.to_list())

        new_rfs = response['risk_factors'].get('new')
        if new_rfs:
            with Timer(f"Inserting riskfactors = {varmodel_id}") as riskfactor_timer:
                max_id = VaRModelRiskfactor.get_max_id()+1
                df = [dict(id=max_id+i, riskfactor_id=rf, model_id=varmodel_id, 
                        created_by=request.user.username, valid_from=current_timestamp, valid_to='2200-12-31T05:00') 
                    for i, rf in enumerate(new_rfs)]
                VaRModelRiskfactor.insert_dataframe(df)

        #Handle Products
        if response['products'].get('deleted'):
            with Timer(f"Updating products = {varmodel_id}") as product_timer:
                deleted_df = VaRModelProduct.get_dataframe(model_id=varmodel_id, product_id=response['products'].get('deleted'), valid_to = '2200-12-31T05:00', pyspark=False)
                if len(deleted_df):
                    VaRModelProduct.update(set_cols=dict(valid_to=current_timestamp), id=deleted_df.id.to_list())

        new_products = response['products'].get('new')
        if new_products:
            with Timer(f"Inserting products = {varmodel_id}") as product_timer:
                max_id = VaRModelProduct.get_max_id()+1
                df = [dict(id=max_id+i, product_id=pid, model_id=varmodel_id, 
                        created_by=request.user.username, valid_from=current_timestamp, valid_to='2200-12-31T05:00') 
                    for i, pid in enumerate(new_products)]
                VaRModelProduct.insert_dataframe(df)

        #Handle Entities
        if response['entities'].get('deleted'):
            with Timer(f"updating Entities = {varmodel_id}") as entity_timer:
                deleted_df = VaRModelEntity.get_dataframe(model_id=varmodel_id, entity_id=response['entities'].get('deleted'), valid_to = '2200-12-31T05:00', pyspark=False)
                if len(deleted_df):
                    VaRModelEntity.update(set_cols=dict(valid_to=current_timestamp), id=deleted_df.id.to_list())

        new_entities = response['entities'].get('new')
        if new_entities:
            with Timer(f"Inserting Entities = {varmodel_id}") as entity_timer:
                max_id = VaRModelEntity.get_max_id()+1
                df = [dict(id=max_id+i, entity_id=eid, model_id=varmodel_id, 
                        created_by=request.user.username, valid_from=current_timestamp, valid_to='2200-12-31T05:00') 
                    for i, eid in enumerate(new_entities)]
            VaRModelEntity.insert_dataframe(df)
    return JsonResponse({"message": "VaR Model Saved successfully...", "status": "success"})

@router.get('/dropdown')
def get_dropdown_data(request, names):
    catalog, layer = os.environ.get('DATABRICKS_CATALOG'), os.environ.get('DATABRICKS_BRONZE_LAYER')
    dropdown_factory = {
        'var_type': f'SELECT VAR_TYPE_ID as value, VAR_TYPE_NM as label FROM {catalog}.{layer}.ref_var_type',
    }
    ret = []
    for name in names.split(','):
        from core.db import DBConnection
        
        with DBConnection() as db:
            qry = dropdown_factory[name]
            df = db.execute(qry, df=True)
            data = df.to_dict(orient='records')
        ret.extend(data)
    return ret
        
@router.post('/workflow/submit')
def trigger_workflow(request):
    azure_container = f"{os.environ.get('AZURE_FRONTEND_VOLUME_STORAGE')}/incoming"
    try:
        with Timer("Triggered workflow/submit api") as timer:
            with AzStorage('USSPARC_BRONZE_VOLUME') as storage:
                # Upload the file to Azure Blob Storage
                payload  = dict(
                    sorted(
                        json.loads(request.body.decode()).items(), key=lambda item: get_sort_key(item[1])
                        )
                    )
                payload_hash = hashlib.md5(str(payload).encode()).hexdigest()[:5]
                current_datetime = str(datetime.datetime.now())
                time_hash = hashlib.md5(current_datetime.encode()).hexdigest()[:5]
                request_id = f"{payload_hash}_{time_hash}"
                storage.write(azure_container, f'{request_id}.json', data=json.dumps(payload))
                logger.info("Writing trigger wrokflow request to Azure storage - Completed...")
            Requests.insert(
                request_id=request_id, request_type='UI',
                request_json=payload, created_by=request.user.username,
                created_at=current_datetime
            )
            return JsonResponse({"message": "Workflow submitted successfully...", "status": "success"})
    except Exception as e:
        logger.error(str(e))
        return JsonResponse({"message": "Error occured during API call...", "status": "error"})

@router.get("/runrequest/all")
def run_request_details(request, request_date=""):
    with Timer("Triggered runrequest/all api") as timer:
        current_date = request_date or str(datetime.datetime.now().date())
        results = Requests.get_dataframe(created_by= [request.user.username, ""], pyspark=False)
        results = results[results['created_at'] >= current_date]
        records = {}
        if len(results):
            run_results = RequestRun.get_dataframe(request_id=results['request_id'].values, pyspark=False)
            records['history'] = []
            if len(run_results):
                run_request_results = pd.merge(results, run_results, on='request_id', how='left').fillna("")
                run_request_results['request_json'] = run_request_results['request_json'].apply(fix_and_parse)
                records['history'] = run_request_results.to_dict(orient='records')
            # records['user_preference'] = json.loads(get_request_by_user(request, request.user.username).content).get('details', {}).get('request_json', {})
        return JsonResponse({
            "status": "success", "message": "Fetched Run Request Details...",
            "details": records,
            "request_date": current_date
        })

@router.get("/runrequest/byuser")
def get_request_by_user(request):
    with Timer("Triggered runrequest/byuser api") as timer:
        results = Requests.get_dataframe(created_by=request.user.username, pyspark=False)
        records = {}
        if len(results):
            results['request_json'] = results['request_json'].apply(fix_and_parse)
            results = results.sort_values(by='created_at', ascending=False)
            records = results.to_dict(orient='records')[0]

        return JsonResponse({
            "status": "success", "message": "Fetched Run Request Details...",
            "details": records.get("request_json", {})
        })


@router.get("/details/")
def get_details(request, model):
    with Timer(f"Triggered details/ api with {model}") as timer:
        models = {'products': Products, 'entities': Entities}
        details = models[model].get_dataframe(pyspark=False)
        return JsonResponse({
            "status": "success", "message": f"Fetched {model} Details..",
            "details": details.to_dict(orient='records')
        })

@router.get("/workflow/export")
def export_workflow(request, request_id):
    with Timer(f"Triggered workflow/export api") as timer:
        response = Requests.get(request_id=request_id).to_dict()
        response_json = json.dumps(ast.literal_eval(response["request_json"]), indent=4)
        request_response = HttpResponse(response_json, content_type="application/json")
        request_response["Content-Disposition"] = f'attachment; filename="{request_id}.json"'

        return request_response

@router.get("/hierarchy/")
def get_hierarchy(request, name):
    with Timer(f"Triggrered hierarchy api for {name}") as timer: 
        hierarchies = {"books": BookHierarchy, "products": ProductHierarchy}
        if name not in hierarchies:
            return JsonResponse({
            "status": "error", "message": "Invalid hierarchy name given. Supported are 'books', 'products'",
            "details": []
            })
        
        #@rcache
        #def get_hierarchy_df(name):
        hierarchy_obj = hierarchies[name]
        with Timer(f"Fetching data for {name}") as timer:
            df = hierarchy_obj.get_dataframe(pyspark=False)
        #return df
        
        #df = get_hierarchy_df(name)
        with Timer(f"Constructing tree for {name}") as timer:
            hierarchy_obj = hierarchies[name]
            filtered_df = df.dropna(subset=list(hierarchy_obj.HIERARCHY))
            if name == "books":
                filtered_df['in_scope'] = ~filtered_df['book'].isin(hierarchy_obj.IN_SCOPE_BOOKS)
                tree_data = build_tree(filtered_df, hierarchy_obj.HIERARCHY, hierarchy_obj.IN_SCOPE_BOOKS)
            else:
                filtered_df['category_product'] = filtered_df['category_elf'] + ' - ' + filtered_df['product_type']
                filtered_df = filtered_df.drop(['category_elf', 'product_type'], axis=1)
                tree_data = build_tree(filtered_df, ("level1", "level2", "category_product"))

        return JsonResponse({
            "status": "success", "message": "Fetched book details successfully...",
            "details": tree_data
            })

@router.get("/var/results/")
def get_var(request, cob_date):
    with Timer(f"Triggered api var/results") as timer:
        if not cob_date:
            from core.db import DBConnection
            with DBConnection() as db:
                schema = db.layer_map['gold']
                query = f"select max(COB_DT) from {schema}.vw_var"
                cob_date = str(db.execute(query, df=True).values[0][0])
        results = VaRResults.get_dataframe(cob_date=cob_date, pyspark=False)
    return JsonResponse({
        "status": "success",
        "message": "Fetched the VaR Results successfully...",
        "details": results.to_dict(orient="records"),
        "cob_date": cob_date
    })

@router.get('/benchmarking-url/')
def get_benchmarking_url(request):
    url = os.getenv("BENCHMARKING_TOOL_URL")
    if not url:
        return JsonResponse({
            "status": "error",
            "message": "Unable to fetch benchmarking url...Please try again after sometime",
            "details": "",
    })
    return JsonResponse({
        "status": "success",
        "message": "Fetched Benchmarking url successfully...",
        "details": url,
    })

#FIXME: this is temp code
@router.get('/pnlstrips')
def get_pnl_strips(request):
    from core.db import DBConnection
    query = "select strips.COB_DT, strips.BOOK_NM, strips.PNL_ID, strips.SCENARIO_ID, strips.SHOCK_DT, strips.PNL_ID, rf.CURVE_NM, rf.RF_CLASS_CD, sum(strips.PNL_AM)/1000 as PNL from rmdad_grc_dev.ussparc_gold.sensitivitypnlstrips strips, rmdad_grc_dev.ussparc_silver.riskfactor rf where COB_DT='2025-08-18' and strips.RF_ID=rf.RF_ID  and rf.VALID_TO_TS = '2200-12-31T05:00' and strips.VALID_TO_TS = '2200-12-31T05:00' group by strips.COB_DT, strips.BOOK_NM, strips.PNL_ID, strips.SCENARIO_ID, strips.SHOCK_DT, strips.PNL_ID, rf.CURVE_NM, rf.RF_CLASS_CD order by strips.SHOCK_DT"
    with DBConnection() as db:
        records = db.execute(query, df=True)
    return JsonResponse({
        "status": "success",
        "message": "Fetched PnL Strips successfully...",
        "details": records.to_dict(orient="records"),
    })

@router.get("/preview")
def get_applicable_factors(request):
    return JsonResponse({
        "sttus": "success",
        "message": "Fetched Applicable Factors successfully...",
    })

def build_tree(df, levels, in_scope_books=None, id_counter=[1]):
    level = levels[0]
    tree = []
    
    for name, group in df.groupby(level):
        node_id = str(id_counter[0])
        id_counter[0] += 1
        node = {'value': node_id, 'label': name}

        if len(levels) > 1:
            children = build_tree(group, levels[1:], in_scope_books, id_counter)
            node['children'] = children
            node['disabled'] = all(child.get('disabled', False) for child in children)
        else:
            if in_scope_books is not None:
                node['disabled'] = name not in in_scope_books
            # else:
            #     node['disabled'] = True

        tree.append(node)
    
    return tree


@router.get('/mktdata/shocks')
def get_riskfactor_shocks(request, rf_id: str, start_date=None, end_date=None):
    from dateutil.parser import parse
    from objects.mktdata.shocks import RiskFactorShocks
    from pandas.tseries.holiday import USFederalHolidayCalendar
    from pandas.tseries.offsets import CustomBusinessDay    
    
    business_days = CustomBusinessDay(calendar=USFederalHolidayCalendar())    
    end_date = end_date or datetime.date.today()
    start_date = start_date or (end_date - business_days*1000)    
    
    
    start_date = parse(start_date) if isinstance(start_date, str) else start_date
    end_date = parse(end_date) if isinstance(end_date, str) else end_date
    rf_id = list(map(int, rf_id.split(',')))
    
    with Timer("Triggered mktdata/shocks api") as timer:
        data = RiskFactorShocks.get_dataframe(risk_factor_id=rf_id, shock_date__gte=start_date, shock_date__lte=end_date, order=['shock_dt__desc'.upper(), 'rf_id'.upper()], pyspark=False)
        
    return JsonResponse({
        "status": "success",
        "message": "Fetched the Risk Factor Shocks successfully...",
        "details": data.to_dict(orient="records")
    })


@router.get('/mktdata/curve')
def get_historic_curve_data(request, rf_id: str, start_date=None, end_date=None):
    from objects.mktdata.riskfactor import RiskFactor   
    from pandas.tseries.holiday import USFederalHolidayCalendar
    from pandas.tseries.offsets import CustomBusinessDay
    
    business_days = CustomBusinessDay(calendar=USFederalHolidayCalendar())    
    end_date = end_date or datetime.date.today()
    start_date = start_date or (end_date - business_days*1000)    
     
    with Timer("Triggered mktdata/shocks api") as timer:
        data = RiskFactor.get_curve_data(rf_id=rf_id, start_date=start_date, end_date=end_date)

    return JsonResponse({
        "status": "success",
        "message": "Fetched the Risk Factor Shocks successfully...",
        "details": data
    })
                
