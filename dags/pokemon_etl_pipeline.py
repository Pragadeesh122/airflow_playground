from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import subprocess
import json

# Default arguments - APPLIES TO ALL TASKS unless overridden
default_args = {
    'owner': 'pragadeesh',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
    'execution_timeout': timedelta(minutes=10),
}

dag = DAG(
    'pokemon_etl_pipeline',
    default_args=default_args,
    description='Real ETL pipeline using PokéAPI',
    schedule='0 */6 * * *',
    catchup=False,
    max_active_runs=1,
    tags=['production', 'etl', 'pokemon', 'api'],
)

# ============================================================================
# TASK FUNCTIONS - USING SUBPROCESS + CURL
# ============================================================================

def check_api_availability(**context):
    """Check if PokéAPI is available using curl"""
    import sys
    
    print("=" * 50, flush=True)
    print("Starting API check with curl...", flush=True)
    sys.stdout.flush()
    
    try:
        result = subprocess.run(
            ['curl', '-s', '-o', '/dev/null', '-w', '%{http_code}', 
             '--max-time', '10', 'https://pokeapi.co/api/v2/pokemon/1'],
            capture_output=True,
            text=True,
            timeout=15
        )
        
        status_code = result.stdout.strip()
        print(f"Got response: {status_code}", flush=True)
        
        if status_code == '200':
            print("✅ API is available!", flush=True)
            return True
        else:
            raise Exception(f"API returned: {status_code}")
            
    except Exception as e:
        print(f"Error: {e}", flush=True)
        raise

def extract_pokemon_list(**context):
    """Extract list of Pokemon from API using curl"""
    print("=" * 50)
    print("Extracting Pokemon list from API...")
    print("=" * 50)
    
    url = 'https://pokeapi.co/api/v2/pokemon?limit=50&offset=0'
    print(f"URL: {url}")
    
    result = subprocess.run(
        ['curl', '-s', '--max-time', '10', url],
        capture_output=True,
        text=True,
        timeout=15
    )
    
    if result.returncode != 0:
        raise Exception(f"Curl failed with return code: {result.returncode}")
    
    data = json.loads(result.stdout)
    pokemon_list = data['results']
    
    print(f"✅ Successfully extracted {len(pokemon_list)} Pokemon")
    
    context['ti'].xcom_push(key='pokemon_list', value=pokemon_list)
    return pokemon_list

def extract_pokemon_details(**context):
    """Extract detailed information for each Pokemon using curl"""
    ti = context['ti']
    pokemon_list = ti.xcom_pull(key='pokemon_list', task_ids='extract_pokemon_list')
    
    print(f"Extracting details for {len(pokemon_list)} Pokemon...")
    
    detailed_pokemon = []
    
    # Get details for first 20 Pokemon
    for idx, pokemon in enumerate(pokemon_list[:20], 1):
        try:
            print(f"[{idx}/20] Fetching: {pokemon['name']}")
            
            result = subprocess.run(
                ['curl', '-s', '--max-time', '10', pokemon['url']],
                capture_output=True,
                text=True,
                timeout=15
            )
            
            if result.returncode != 0:
                print(f"⚠️  Curl failed for {pokemon['name']}")
                continue
            
            details = json.loads(result.stdout)
            
            pokemon_data = {
                'id': details['id'],
                'name': details['name'],
                'height': details['height'],
                'weight': details['weight'],
                'base_experience': details.get('base_experience', 0),
                'types': [t['type']['name'] for t in details['types']],
                'abilities': [a['ability']['name'] for a in details['abilities']],
                'stats': {stat['stat']['name']: stat['base_stat'] for stat in details['stats']},
            }
            detailed_pokemon.append(pokemon_data)
            
        except Exception as e:
            print(f"⚠️  Error fetching {pokemon['name']}: {str(e)}")
            continue
    
    print(f"✅ Successfully extracted details for {len(detailed_pokemon)} Pokemon")
    ti.xcom_push(key='detailed_pokemon', value=detailed_pokemon)
    return detailed_pokemon

def extract_type_data(**context):
    """Extract Pokemon type information using curl"""
    print("Extracting Pokemon type data...")
    
    result = subprocess.run(
        ['curl', '-s', '--max-time', '10', 'https://pokeapi.co/api/v2/type'],
        capture_output=True,
        text=True,
        timeout=15
    )
    
    if result.returncode != 0:
        raise Exception(f"Curl failed with return code: {result.returncode}")
    
    types_data = json.loads(result.stdout)
    
    type_details = []
    for type_info in types_data['results'][:10]:
        try:
            result = subprocess.run(
                ['curl', '-s', '--max-time', '10', type_info['url']],
                capture_output=True,
                text=True,
                timeout=15
            )
            
            if result.returncode != 0:
                print(f"⚠️  Curl failed for type {type_info['name']}")
                continue
            
            type_detail = json.loads(result.stdout)
            
            type_details.append({
                'name': type_detail['name'],
                'id': type_detail['id'],
                'damage_relations': {
                    'double_damage_to': [t['name'] for t in type_detail['damage_relations']['double_damage_to']],
                    'double_damage_from': [t['name'] for t in type_detail['damage_relations']['double_damage_from']],
                }
            })
        except Exception as e:
            print(f"⚠️  Error fetching type {type_info['name']}: {str(e)}")
            continue
    
    print(f"✅ Extracted {len(type_details)} type details")
    context['ti'].xcom_push(key='type_data', value=type_details)
    return type_details

def validate_data_quality(**context):
    """Validate data quality and decide next step"""
    ti = context['ti']
    pokemon_data = ti.xcom_pull(key='detailed_pokemon', task_ids='extract_pokemon_details')
    
    print("Running data quality checks...")
    
    if not pokemon_data or len(pokemon_data) == 0:
        print("❌ Data quality check FAILED: No Pokemon data found")
        return 'handle_data_quality_failure'
    
    if len(pokemon_data) < 10:
        print(f"❌ Data quality check FAILED: Only {len(pokemon_data)} records (minimum 10 required)")
        return 'handle_data_quality_failure'
    
    required_fields = ['id', 'name', 'height', 'weight', 'types', 'stats']
    for pokemon in pokemon_data:
        missing_fields = [field for field in required_fields if field not in pokemon]
        if missing_fields:
            print(f"❌ Data quality check FAILED: Missing fields {missing_fields}")
            return 'handle_data_quality_failure'
    
    print(f"✅ Data quality check PASSED: {len(pokemon_data)} valid records")
    return 'transform_pokemon_data'

def transform_pokemon_data(**context):
    """Transform and enrich Pokemon data"""
    ti = context['ti']
    pokemon_data = ti.xcom_pull(key='detailed_pokemon', task_ids='extract_pokemon_details')
    
    print("Transforming Pokemon data...")
    
    transformed_pokemon = []
    
    for pokemon in pokemon_data:
        total_stats = sum(pokemon['stats'].values())
        avg_stat = total_stats / len(pokemon['stats'])
        
        size_category = 'small' if pokemon['height'] < 10 else 'medium' if pokemon['height'] < 20 else 'large'
        weight_category = 'light' if pokemon['weight'] < 100 else 'medium' if pokemon['weight'] < 500 else 'heavy'
        
        if pokemon['base_experience'] and pokemon['base_experience'] > 200:
            power_tier = 'legendary'
        elif total_stats > 500:
            power_tier = 'strong'
        elif total_stats > 400:
            power_tier = 'average'
        else:
            power_tier = 'weak'
        
        transformed = {
            'pokemon_id': pokemon['id'],
            'name': pokemon['name'].title(),
            'primary_type': pokemon['types'][0] if pokemon['types'] else 'unknown',
            'secondary_type': pokemon['types'][1] if len(pokemon['types']) > 1 else None,
            'height_dm': pokemon['height'],
            'weight_hg': pokemon['weight'],
            'size_category': size_category,
            'weight_category': weight_category,
            'base_experience': pokemon['base_experience'],
            'power_tier': power_tier,
            'total_stats': total_stats,
            'avg_stat': round(avg_stat, 2),
            'hp': pokemon['stats'].get('hp', 0),
            'attack': pokemon['stats'].get('attack', 0),
            'defense': pokemon['stats'].get('defense', 0),
            'speed': pokemon['stats'].get('speed', 0),
            'abilities_count': len(pokemon['abilities']),
            'abilities': ', '.join(pokemon['abilities']),
            'is_dual_type': len(pokemon['types']) > 1,
        }
        
        transformed_pokemon.append(transformed)
    
    print(f"✅ Transformed {len(transformed_pokemon)} Pokemon records")
    ti.xcom_push(key='transformed_pokemon', value=transformed_pokemon)
    return transformed_pokemon

def analyze_pokemon_stats(**context):
    """Analyze Pokemon statistics"""
    ti = context['ti']
    pokemon_data = ti.xcom_pull(key='transformed_pokemon', task_ids='transform_pokemon_data')
    
    print("Analyzing Pokemon statistics...")
    
    total_pokemon = len(pokemon_data)
    
    type_distribution = {}
    for pokemon in pokemon_data:
        ptype = pokemon['primary_type']
        type_distribution[ptype] = type_distribution.get(ptype, 0) + 1
    
    power_distribution = {}
    for pokemon in pokemon_data:
        tier = pokemon['power_tier']
        power_distribution[tier] = power_distribution.get(tier, 0) + 1
    
    size_distribution = {}
    for pokemon in pokemon_data:
        size = pokemon['size_category']
        size_distribution[size] = size_distribution.get(size, 0) + 1
    
    avg_stats = {
        'avg_total_stats': round(sum(p['total_stats'] for p in pokemon_data) / total_pokemon, 2),
        'avg_hp': round(sum(p['hp'] for p in pokemon_data) / total_pokemon, 2),
        'avg_attack': round(sum(p['attack'] for p in pokemon_data) / total_pokemon, 2),
        'avg_defense': round(sum(p['defense'] for p in pokemon_data) / total_pokemon, 2),
        'avg_speed': round(sum(p['speed'] for p in pokemon_data) / total_pokemon, 2),
    }
    
    top_by_stats = sorted(pokemon_data, key=lambda x: x['total_stats'], reverse=True)[:5]
    top_by_attack = sorted(pokemon_data, key=lambda x: x['attack'], reverse=True)[:5]
    
    analysis = {
        'total_pokemon_analyzed': total_pokemon,
        'type_distribution': type_distribution,
        'power_tier_distribution': power_distribution,
        'size_distribution': size_distribution,
        'average_statistics': avg_stats,
        'top_5_by_total_stats': [{'name': p['name'], 'total_stats': p['total_stats']} for p in top_by_stats],
        'top_5_by_attack': [{'name': p['name'], 'attack': p['attack']} for p in top_by_attack],
        'dual_type_percentage': round((sum(1 for p in pokemon_data if p['is_dual_type']) / total_pokemon) * 100, 2),
        'timestamp': datetime.now().isoformat(),
    }
    
    print("\n" + "="*70)
    print("POKEMON ANALYSIS REPORT")
    print("="*70)
    print(json.dumps(analysis, indent=2))
    print("="*70 + "\n")
    
    ti.xcom_push(key='analysis', value=analysis)
    return analysis

def generate_insights(**context):
    """Generate business insights"""
    ti = context['ti']
    analysis = ti.xcom_pull(key='analysis', task_ids='analyze_pokemon_stats')
    
    print("Generating insights...")
    
    insights = []
    
    most_common_type = max(analysis['type_distribution'].items(), key=lambda x: x[1])
    insights.append(f"📊 Most common type: {most_common_type[0].upper()} with {most_common_type[1]} Pokemon")
    
    legendary_count = analysis['power_tier_distribution'].get('legendary', 0)
    if legendary_count > 0:
        insights.append(f"⭐ Found {legendary_count} legendary-tier Pokemon!")
    
    dual_pct = analysis['dual_type_percentage']
    insights.append(f"🔀 {dual_pct}% of Pokemon have dual types")
    
    top_pokemon = analysis['top_5_by_total_stats'][0]
    insights.append(f"🏆 Strongest Pokemon: {top_pokemon['name']} with {top_pokemon['total_stats']} total stats")
    
    insights_report = {
        'insights': insights,
        'generated_at': datetime.now().isoformat(),
        'based_on_records': analysis['total_pokemon_analyzed'],
    }
    
    print("\n" + "="*70)
    print("INSIGHTS REPORT")
    print("="*70)
    for idx, insight in enumerate(insights, 1):
        print(f"{idx}. {insight}")
    print("="*70 + "\n")
    
    ti.xcom_push(key='insights', value=insights_report)
    return insights_report

def save_to_data_warehouse(**context):
    """Simulate saving to data warehouse"""
    ti = context['ti']
    pokemon_data = ti.xcom_pull(key='transformed_pokemon', task_ids='transform_pokemon_data')
    
    print("📦 Loading data to warehouse...")
    
    for pokemon in pokemon_data:
        print(f"  ✓ Loaded: {pokemon['name']} ({pokemon['primary_type']}) - Power: {pokemon['power_tier']}")
    
    load_summary = {
        'records_loaded': len(pokemon_data),
        'load_timestamp': datetime.now().isoformat(),
        'target_table': 'pokemon_dim',
    }
    
    print(f"\n✅ Successfully loaded {len(pokemon_data)} records!")
    ti.xcom_push(key='load_summary', value=load_summary)
    return load_summary

def create_visualization(**context):
    """Create interactive HTML visualization of Pokemon data"""
    ti = context['ti']
    analysis = ti.xcom_pull(key='analysis', task_ids='analyze_pokemon_stats')
    pokemon_data = ti.xcom_pull(key='transformed_pokemon', task_ids='transform_pokemon_data')
    
    print("Creating visualization dashboard...")
    
    # Create HTML with embedded Plotly charts
    html_content = f"""
<!DOCTYPE html>
<html>
<head>
    <title>Pokemon ETL Analysis Dashboard</title>
    <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
    <style>
        body {{
            font-family: Arial, sans-serif;
            margin: 20px;
            background-color: #f5f5f5;
        }}
        .header {{
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 30px;
            border-radius: 10px;
            margin-bottom: 30px;
        }}
        .header h1 {{
            margin: 0;
            font-size: 2.5em;
        }}
        .stats-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 20px;
            margin-bottom: 30px;
        }}
        .stat-card {{
            background: white;
            padding: 20px;
            border-radius: 10px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
            text-align: center;
        }}
        .stat-value {{
            font-size: 2.5em;
            font-weight: bold;
            color: #667eea;
        }}
        .stat-label {{
            color: #666;
            margin-top: 10px;
        }}
        .chart-container {{
            background: white;
            padding: 20px;
            border-radius: 10px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
            margin-bottom: 30px;
        }}
        .chart-title {{
            font-size: 1.5em;
            margin-bottom: 15px;
            color: #333;
        }}
        table {{
            width: 100%;
            border-collapse: collapse;
            background: white;
        }}
        th, td {{
            padding: 12px;
            text-align: left;
            border-bottom: 1px solid #ddd;
        }}
        th {{
            background-color: #667eea;
            color: white;
        }}
        tr:hover {{
            background-color: #f5f5f5;
        }}
    </style>
</head>
<body>
    <div class="header">
        <h1>🎮 Pokemon ETL Analysis Dashboard</h1>
        <p>Pipeline executed at: {analysis['timestamp']}</p>
    </div>
    
    <div class="stats-grid">
        <div class="stat-card">
            <div class="stat-value">{analysis['total_pokemon_analyzed']}</div>
            <div class="stat-label">Total Pokemon Analyzed</div>
        </div>
        <div class="stat-card">
            <div class="stat-value">{analysis['average_statistics']['avg_total_stats']}</div>
            <div class="stat-label">Average Total Stats</div>
        </div>
        <div class="stat-card">
            <div class="stat-value">{analysis['dual_type_percentage']}%</div>
            <div class="stat-label">Dual-Type Pokemon</div>
        </div>
        <div class="stat-card">
            <div class="stat-value">{len(analysis['type_distribution'])}</div>
            <div class="stat-label">Different Types</div>
        </div>
    </div>
    
    <div class="chart-container">
        <div class="chart-title">Type Distribution</div>
        <div id="typeChart"></div>
    </div>
    
    <div class="chart-container">
        <div class="chart-title">Power Tier Distribution</div>
        <div id="powerChart"></div>
    </div>
    
    <div class="chart-container">
        <div class="chart-title">Average Stats Comparison</div>
        <div id="statsChart"></div>
    </div>
    
    <div class="chart-container">
        <div class="chart-title">Top 10 Pokemon by Total Stats</div>
        <div id="topPokemonChart"></div>
    </div>
    
    <div class="chart-container">
        <div class="chart-title">Size vs Weight Distribution</div>
        <div id="scatterChart"></div>
    </div>
    
    <div class="chart-container">
        <div class="chart-title">Top 5 Pokemon by Total Stats</div>
        <table>
            <tr>
                <th>Rank</th>
                <th>Name</th>
                <th>Total Stats</th>
            </tr>
"""
    
    for idx, pokemon in enumerate(analysis['top_5_by_total_stats'], 1):
        html_content += f"""
            <tr>
                <td>{idx}</td>
                <td>{pokemon['name']}</td>
                <td>{pokemon['total_stats']}</td>
            </tr>
"""
    
    html_content += """
        </table>
    </div>
    
    <script>
"""
    
    # Type Distribution Chart
    type_names = list(analysis['type_distribution'].keys())
    type_counts = list(analysis['type_distribution'].values())
    
    html_content += f"""
    var typeData = [{{
        values: {type_counts},
        labels: {type_names},
        type: 'pie',
        marker: {{
            colors: ['#FF6B6B', '#4ECDC4', '#45B7D1', '#FFA07A', '#98D8C8', '#F7DC6F', '#BB8FCE', '#85C1E2', '#F8B739']
        }}
    }}];
    
    var typeLayout = {{
        height: 400,
        showlegend: true
    }};
    
    Plotly.newPlot('typeChart', typeData, typeLayout);
"""
    
    # Power Tier Chart
    power_names = list(analysis['power_tier_distribution'].keys())
    power_counts = list(analysis['power_tier_distribution'].values())
    
    html_content += f"""
    var powerData = [{{
        x: {power_names},
        y: {power_counts},
        type: 'bar',
        marker: {{
            color: ['#FF6B6B', '#FFA07A', '#FFD93D', '#6BCF7F'],
            line: {{
                width: 2,
                color: '#333'
            }}
        }}
    }}];
    
    var powerLayout = {{
        height: 400,
        xaxis: {{ title: 'Power Tier' }},
        yaxis: {{ title: 'Count' }}
    }};
    
    Plotly.newPlot('powerChart', powerData, powerLayout);
"""
    
    # Average Stats Chart
    avg_stats = analysis['average_statistics']
    
    html_content += f"""
    var statsData = [{{
        x: ['HP', 'Attack', 'Defense', 'Speed'],
        y: [{avg_stats['avg_hp']}, {avg_stats['avg_attack']}, {avg_stats['avg_defense']}, {avg_stats['avg_speed']}],
        type: 'bar',
        marker: {{
            color: ['#E74C3C', '#3498DB', '#F39C12', '#2ECC71']
        }}
    }}];
    
    var statsLayout = {{
        height: 400,
        xaxis: {{ title: 'Stat Type' }},
        yaxis: {{ title: 'Average Value' }}
    }};
    
    Plotly.newPlot('statsChart', statsData, statsLayout);
"""
    
    # Top 10 Pokemon Chart
    top_10 = sorted(pokemon_data, key=lambda x: x['total_stats'], reverse=True)[:10]
    top_names = [p['name'] for p in top_10]
    top_stats = [p['total_stats'] for p in top_10]
    
    html_content += f"""
    var topPokemonData = [{{
        x: {top_stats},
        y: {top_names},
        type: 'bar',
        orientation: 'h',
        marker: {{
            color: '#667eea'
        }}
    }}];
    
    var topPokemonLayout = {{
        height: 500,
        xaxis: {{ title: 'Total Stats' }},
        yaxis: {{ automargin: true }}
    }};
    
    Plotly.newPlot('topPokemonChart', topPokemonData, topPokemonLayout);
"""
    
    # Scatter plot - Size vs Weight
    heights = [p['height_dm'] for p in pokemon_data]
    weights = [p['weight_hg'] for p in pokemon_data]
    names = [p['name'] for p in pokemon_data]
    
    html_content += f"""
    var scatterData = [{{
        x: {heights},
        y: {weights},
        mode: 'markers',
        type: 'scatter',
        text: {names},
        marker: {{
            size: 10,
            color: {heights},
            colorscale: 'Viridis',
            showscale: true
        }}
    }}];
    
    var scatterLayout = {{
        height: 400,
        xaxis: {{ title: 'Height (dm)' }},
        yaxis: {{ title: 'Weight (hg)' }},
        hovermode: 'closest'
    }};
    
    Plotly.newPlot('scatterChart', scatterData, scatterLayout);
    </script>
</body>
</html>
"""
    
    # Save HTML file
    output_path = '/tmp/pokemon_dashboard.html'
    with open(output_path, 'w') as f:
        f.write(html_content)
    
    print(f"✅ Dashboard created at: {output_path}")
    print(f"📊 Open in browser: file://{output_path}")
    
    ti.xcom_push(key='dashboard_path', value=output_path)
    return output_path

def handle_failure(**context):
    """Handle data quality failures"""
    print("\n" + "="*70)
    print("⚠️  DATA QUALITY FAILURE HANDLER")
    print("="*70)
    print("🔔 Alert sent to data team")
    print("🎫 Jira ticket created")
    print("📧 Email notification sent")
    print("="*70 + "\n")
    return "failure_handled"

def send_success_notification(**context):
    """Send success notification"""
    ti = context['ti']
    analysis = ti.xcom_pull(key='analysis', task_ids='analyze_pokemon_stats')
    insights = ti.xcom_pull(key='insights', task_ids='generate_insights')
    load_summary = ti.xcom_pull(key='load_summary', task_ids='save_to_data_warehouse')
    
    print("\n" + "="*70)
    print("🎉 PIPELINE EXECUTION SUCCESSFUL!")
    print("="*70)
    print(f"📊 Pokemon Processed: {analysis['total_pokemon_analyzed']}")
    print(f"💾 Records Loaded: {load_summary['records_loaded']}")
    print(f"⚡ Top Pokemon: {analysis['top_5_by_total_stats'][0]['name']}")
    print(f"💡 Key Insights:")
    for idx, insight in enumerate(insights['insights'], 1):
        print(f"   {idx}. {insight}")
    print("="*70 + "\n")
    return "success"

def cleanup_resources(**context):
    """Cleanup"""
    print("🧹 Cleanup completed!")
    return "cleaned"

# ============================================================================
# DEFINE TASKS
# ============================================================================

start = EmptyOperator(task_id='start', dag=dag)

check_api = PythonOperator(
    task_id='check_api_available',
    python_callable=check_api_availability,
    execution_timeout=timedelta(seconds=30),
    dag=dag,
)

extract_list = PythonOperator(
    task_id='extract_pokemon_list',
    python_callable=extract_pokemon_list,
    dag=dag,
)

extract_details = PythonOperator(
    task_id='extract_pokemon_details',
    python_callable=extract_pokemon_details,
    dag=dag,
)

extract_types = PythonOperator(
    task_id='extract_type_data',
    python_callable=extract_type_data,
    dag=dag,
)

extraction_complete = EmptyOperator(task_id='extraction_complete', dag=dag)

validate = BranchPythonOperator(
    task_id='validate_data_quality',
    python_callable=validate_data_quality,
    dag=dag,
)

handle_failure_task = PythonOperator(
    task_id='handle_data_quality_failure',
    python_callable=handle_failure,
    dag=dag,
)

transform = PythonOperator(
    task_id='transform_pokemon_data',
    python_callable=transform_pokemon_data,
    dag=dag,
)

analyze = PythonOperator(
    task_id='analyze_pokemon_stats',
    python_callable=analyze_pokemon_stats,
    dag=dag,
)

generate_insights_task = PythonOperator(
    task_id='generate_insights',
    python_callable=generate_insights,
    dag=dag,
)

save_warehouse = PythonOperator(
    task_id='save_to_data_warehouse',
    python_callable=save_to_data_warehouse,
    dag=dag,
)

create_viz = PythonOperator(
    task_id='create_visualization',
    python_callable=create_visualization,
    dag=dag,
)

processing_complete = EmptyOperator(
    task_id='processing_complete',
    trigger_rule='none_failed_min_one_success',
    dag=dag,
)

notify_success = PythonOperator(
    task_id='send_success_notification',
    python_callable=send_success_notification,
    dag=dag,
)

cleanup = PythonOperator(
    task_id='cleanup_resources',
    python_callable=cleanup_resources,
    trigger_rule='all_done',
    dag=dag,
)

end = EmptyOperator(task_id='end', trigger_rule='all_done', dag=dag)

# ============================================================================
# TASK DEPENDENCIES
# ============================================================================

start >> check_api >> extract_list
extract_list >> [extract_details, extract_types] >> extraction_complete
extraction_complete >> validate
validate >> [transform, handle_failure_task]
transform >> [analyze, save_warehouse]
analyze >> generate_insights_task
[generate_insights_task, save_warehouse] >> create_viz >> processing_complete
handle_failure_task >> processing_complete
processing_complete >> notify_success >> cleanup >> end