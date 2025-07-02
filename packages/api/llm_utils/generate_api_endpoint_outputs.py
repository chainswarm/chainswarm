#!/usr/bin/env python3
"""
Script to generate endpoint outputs from OpenAPI specification.

This script:
1. Reads the OpenAPI specification from llm/openapi.json
2. Dynamically parses all endpoints and extracts parameters
3. Executes each endpoint with example values
4. Saves responses to JSON files with proper naming convention
5. Handles different parameter types and error cases
"""

import json
import os
import sys
import requests
import urllib.parse
from typing import Dict, List, Any, Optional, Tuple
import logging
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class EndpointOutputGenerator:
    def __init__(self, openapi_file: str = "openapi.json", base_url: Optional[str] = None,
                 use_local_only: bool = False):
        """
        Initialize the endpoint output generator.

        Args:
            openapi_file: Path to the OpenAPI specification file (relative to script location)
            base_url: Base URL for the API (can be set via API_BASE_URL env var)
            use_local_only: If True, only use local file, don't fetch from API
        """
        # Get the directory where this script is located
        script_dir = Path(__file__).parent

        # If openapi_file is just a filename, look for it in the same directory as the script
        if not os.path.sep in openapi_file and not os.path.altsep in openapi_file:
            self.openapi_file = script_dir / openapi_file
        else:
            self.openapi_file = openapi_file

        self.base_url = base_url or os.getenv('API_BASE_URL', 'http://localhost:8000')
        self.use_local_only = use_local_only
        self.openapi_spec = None
        self.session = requests.Session()
        self.session.timeout = 30

        # Output to the same directory as the script (llm directory)
        self.output_dir = script_dir

        # Track success/failure
        self.successful_endpoints = []
        self.failed_endpoints = []

    def fetch_openapi_spec_from_api(self) -> None:
        """Fetch the OpenAPI specification from the API and save it to disk."""
        try:
            openapi_url = f"{self.base_url.rstrip('/')}/openapi.json"
            logger.info(f"Fetching OpenAPI spec from {openapi_url}")

            response = self.session.get(openapi_url)
            response.raise_for_status()

            self.openapi_spec = response.json()

            # Save to disk
            with open(self.openapi_file, 'w', encoding='utf-8') as f:
                json.dump(self.openapi_spec, f, indent=2, ensure_ascii=False)

            logger.info(f"OpenAPI spec fetched and saved to {self.openapi_file}")

        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch OpenAPI spec from API: {e}")
            # Try to load from existing file as fallback
            self.load_openapi_spec_from_file()
        except Exception as e:
            logger.error(f"Failed to process OpenAPI spec: {e}")
            sys.exit(1)

    def load_openapi_spec_from_file(self) -> None:
        """Load the OpenAPI specification from file (fallback method)."""
        try:
            with open(self.openapi_file, 'r', encoding='utf-8') as f:
                self.openapi_spec = json.load(f)
            logger.info(f"Loaded OpenAPI spec from existing file {self.openapi_file}")
        except Exception as e:
            logger.error(f"Failed to load OpenAPI spec from file: {e}")
            logger.error("Cannot proceed without OpenAPI specification")
            sys.exit(1)

    def load_openapi_spec(self) -> None:
        """Load the OpenAPI specification - first try API, then fallback to file."""
        if self.use_local_only:
            logger.info("Using local-only mode, loading from file")
            self.load_openapi_spec_from_file()
        else:
            self.fetch_openapi_spec_from_api()

    def extract_endpoints(self) -> List[Dict[str, Any]]:
        """Extract all endpoints from the OpenAPI specification."""
        endpoints = []

        if not self.openapi_spec or 'paths' not in self.openapi_spec:
            logger.error("Invalid OpenAPI specification")
            return endpoints

        for path, path_info in self.openapi_spec['paths'].items():
            for method, endpoint_info in path_info.items():
                if method.upper() in ['GET', 'POST', 'PUT', 'DELETE', 'PATCH']:
                    endpoint = {
                        'path': path,
                        'method': method.upper(),
                        'operation_id': endpoint_info.get('operationId', ''),
                        'summary': endpoint_info.get('summary', ''),
                        'tags': endpoint_info.get('tags', []),
                        'parameters': endpoint_info.get('parameters', []),
                        'request_body': endpoint_info.get('requestBody', None)
                    }
                    endpoints.append(endpoint)

        logger.info(f"Extracted {len(endpoints)} endpoints")
        return endpoints

    def get_parameter_example_value(self, param: Dict[str, Any]) -> Any:
        """Get example value for a parameter."""
        # First try the example field
        if 'example' in param:
            return param['example']

        # Then try schema example
        schema = param.get('schema', {})
        if 'example' in schema:
            return schema['example']

        # Generate default values based on type
        param_type = schema.get('type', 'string')

        if param_type == 'string':
            # Special handling for specific parameter names
            param_name = param.get('name', '').lower()
            if 'network' in param_name:
                return 'torus'
            elif 'address' in param_name:
                return '5C4n8kb3mno7i8vQmqNgsQbwZozHvPyou8TAfZfZ7msTkS5f'
            elif 'period' in param_name:
                pattern = schema.get('pattern', '')
                if 'daily|weekly|monthly' in pattern:
                    return 'daily'
                elif '4hour|daily|weekly|monthly' in pattern:
                    return '4hour'
            elif 'transaction_id' in param_name:
                return '308-0001'
            elif 'direction' in param_name:
                return 'all'
            elif 'query_type' in param_name:
                return 'by_address'
            elif 'embedding_type' in param_name:
                return 'network'
            elif 'similarity_metric' in param_name:
                return 'cosine'
            elif 'address_type' in param_name:
                return 'Exchange'
            elif 'date' in param_name:
                return '2024-01-01'
            return 'default_string'

        elif param_type == 'integer':
            param_name = param.get('name', '').lower()
            if 'page' in param_name:
                return 1
            elif 'size' in param_name:
                return 20
            elif 'depth' in param_name:
                return 3
            elif 'block_height' in param_name:
                return 308
            elif 'timestamp' in param_name:
                return 1640995200000
            elif 'limit' in param_name:
                return 10
            elif 'threshold' in param_name:
                return 1000
            return schema.get('default', 1)

        elif param_type == 'number':
            param_name = param.get('name', '').lower()
            if 'similarity_score' in param_name:
                return 0.5  # Valid similarity score between 0 and 1
            elif 'threshold' in param_name:
                return 1000.0
            return schema.get('default', 1000.0)

        elif param_type == 'boolean':
            return schema.get('default', False)

        elif param_type == 'array':
            items_type = schema.get('items', {}).get('type', 'string')
            if items_type == 'string':
                param_name = param.get('name', '').lower()
                if 'address' in param_name:
                    return ['5C4n8kb3mno7i8vQmqNgsQbwZozHvPyou8TAfZfZ7msTkS5f',
                            '5FZdduraHpWTVFBehbH4yqsfi7LabXFQkmqc2vKqbQTaspwM']
                elif 'asset' in param_name:
                    return ['TOR']
                return ['default_item']
            return [1]

        return None

    def build_request_url_and_params(self, endpoint: Dict[str, Any]) -> Tuple[str, Dict[str, Any], Dict[str, Any]]:
        """Build the request URL and separate path/query parameters."""
        path = endpoint['path']
        path_params = {}
        query_params = {}

        for param in endpoint['parameters']:
            param_name = param['name']
            param_in = param['in']
            example_value = self.get_parameter_example_value(param)

            if example_value is not None:
                if param_in == 'path':
                    path_params[param_name] = example_value
                elif param_in == 'query':
                    # Handle array parameters
                    if isinstance(example_value, list):
                        query_params[param_name] = example_value
                    else:
                        query_params[param_name] = example_value

        # Replace path parameters
        url_path = path
        for param_name, param_value in path_params.items():
            url_path = url_path.replace(f'{{{param_name}}}', str(param_value))

        full_url = f"{self.base_url.rstrip('/')}{url_path}"

        return full_url, query_params, path_params

    def build_request_body(self, endpoint: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Build request body for POST requests."""
        if not endpoint.get('request_body'):
            return None

        request_body = endpoint['request_body']
        content = request_body.get('content', {})
        json_content = content.get('application/json', {})
        schema_ref = json_content.get('schema', {}).get('$ref', '')

        # Handle similarity search POST body
        if 'similarity-search' in endpoint['path']:
            # For by_address query type, return minimal body
            return {}

        return {}

    def generate_filename(self, endpoint: Dict[str, Any], path_params: Dict[str, Any]) -> str:
        """Generate appropriate filename based on endpoint path and tags."""
        path = endpoint['path']
        tags = endpoint.get('tags', [])
        method = endpoint['method'].lower()

        # Extract main category from path
        path_parts = [p for p in path.split('/') if p and not p.startswith('{')]

        # Determine prefix based on tags or path
        if 'money-flow' in tags:
            prefix = 'money_flow'
        elif 'balance-series' in tags:
            prefix = 'balance_series'
        elif 'balance-transfers' in tags:
            prefix = 'balance_transfers'
        elif 'similarity-search' in tags:
            prefix = 'similarity_search'
        elif 'known-addresses' in tags:
            prefix = 'known_addresses'
        else:
            # For metrics, health, etc.
            if len(path_parts) > 0:
                prefix = path_parts[0].replace('-', '_')
            else:
                prefix = 'endpoint'

        # Build descriptive name from path
        name_parts = []
        for part in path_parts[1:]:  # Skip the first part as it's usually the network
            if part not in ['substrate']:
                name_parts.append(part.replace('-', '_'))

        # Add path parameters to make filename unique
        for key, value in path_params.items():
            if key != 'network':  # Skip network as it's common
                name_parts.append(f"{key}_{value}".replace('-', '_'))

        # Combine parts
        if name_parts:
            endpoint_name = '_'.join(name_parts)
        else:
            endpoint_name = method

        filename = f"{prefix}_{endpoint_name}.json"

        # Clean up filename
        filename = filename.replace('__', '_').replace('/', '_')

        return filename

    def execute_endpoint(self, endpoint: Dict[str, Any]) -> None:
        """Execute a single endpoint and save the response."""
        try:
            url, query_params, path_params = self.build_request_url_and_params(endpoint)
            request_body = self.build_request_body(endpoint)
            method = endpoint['method']

            logger.info(f"Executing {method} {url}")
            logger.debug(f"Query params: {query_params}")
            logger.debug(f"Request body: {request_body}")

            # Make the request
            if method == 'GET':
                response = self.session.get(url, params=query_params)
            elif method == 'POST':
                response = self.session.post(url, params=query_params, json=request_body)
            else:
                logger.warning(f"Unsupported method {method} for {url}")
                return

            # Generate filename
            filename = self.generate_filename(endpoint, path_params)
            filepath = self.output_dir / filename

            # Prepare response data
            response_data = {
                'endpoint': {
                    'url': url,
                    'method': method,
                    'path': endpoint['path'],
                    'operation_id': endpoint.get('operation_id', ''),
                    'summary': endpoint.get('summary', ''),
                    'tags': endpoint.get('tags', [])
                },
                'request': {
                    'query_params': query_params,
                    'path_params': path_params,
                    'body': request_body
                },
                'response': {
                    'status_code': response.status_code,
                    'headers': dict(response.headers),
                    'content_type': response.headers.get('content-type', ''),
                }
            }

            # Try to parse JSON response
            try:
                if response.headers.get('content-type', '').startswith('application/json'):
                    response_data['response']['data'] = response.json()
                else:
                    response_data['response']['text'] = response.text
            except json.JSONDecodeError:
                response_data['response']['text'] = response.text

            # Save to file
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(response_data, f, indent=2, ensure_ascii=False)

            if response.status_code < 400:
                self.successful_endpoints.append({
                    'endpoint': f"{method} {endpoint['path']}",
                    'file': filename,
                    'status': response.status_code
                })
                logger.info(f"✓ Success: {method} {endpoint['path']} -> {filename}")
            else:
                self.failed_endpoints.append({
                    'endpoint': f"{method} {endpoint['path']}",
                    'file': filename,
                    'status': response.status_code,
                    'error': response.text[:200] if response.text else 'No error message'
                })
                logger.warning(f"⚠ HTTP {response.status_code}: {method} {endpoint['path']} -> {filename}")

        except requests.exceptions.RequestException as e:
            self.failed_endpoints.append({
                'endpoint': f"{method} {endpoint['path']}",
                'file': 'N/A',
                'status': 'Network Error',
                'error': str(e)
            })
            logger.error(f"✗ Network error for {method} {endpoint['path']}: {e}")
        except Exception as e:
            self.failed_endpoints.append({
                'endpoint': f"{method} {endpoint['path']}",
                'file': 'N/A',
                'status': 'Script Error',
                'error': str(e)
            })
            logger.error(f"✗ Script error for {method} {endpoint['path']}: {e}")

    def generate_summary_report(self) -> None:
        """Generate a summary report of all endpoint executions."""
        summary = {
            'execution_summary': {
                'total_endpoints': len(self.successful_endpoints) + len(self.failed_endpoints),
                'successful': len(self.successful_endpoints),
                'failed': len(self.failed_endpoints),
                'success_rate': len(self.successful_endpoints) / (
                            len(self.successful_endpoints) + len(self.failed_endpoints)) * 100 if (
                                                                                                              len(self.successful_endpoints) + len(
                                                                                                          self.failed_endpoints)) > 0 else 0
            },
            'successful_endpoints': self.successful_endpoints,
            'failed_endpoints': self.failed_endpoints,
            'configuration': {
                'base_url': self.base_url,
                'openapi_file': str(self.openapi_file),
                'output_directory': str(self.output_dir)
            }
        }

        # Save summary report
        summary_file = self.output_dir / 'execution_summary.json'
        with open(summary_file, 'w', encoding='utf-8') as f:
            json.dump(summary, f, indent=2, ensure_ascii=False)

        # Print summary to console
        print("\n" + "=" * 60)
        print("ENDPOINT EXECUTION SUMMARY")
        print("=" * 60)
        print(f"Total endpoints: {summary['execution_summary']['total_endpoints']}")
        print(f"Successful: {summary['execution_summary']['successful']}")
        print(f"Failed: {summary['execution_summary']['failed']}")
        print(f"Success rate: {summary['execution_summary']['success_rate']:.1f}%")
        print(f"Base URL: {self.base_url}")
        print(f"Output directory: {self.output_dir}")

        if self.successful_endpoints:
            print(f"\n✓ SUCCESSFUL ENDPOINTS ({len(self.successful_endpoints)}):")
            for endpoint in self.successful_endpoints:
                print(f"  {endpoint['endpoint']} -> {endpoint['file']} (HTTP {endpoint['status']})")

        if self.failed_endpoints:
            print(f"\n✗ FAILED ENDPOINTS ({len(self.failed_endpoints)}):")
            for endpoint in self.failed_endpoints:
                print(f"  {endpoint['endpoint']} -> {endpoint.get('file', 'N/A')} ({endpoint['status']})")
                if endpoint.get('error'):
                    print(f"    Error: {endpoint['error']}")

        print(f"\nDetailed results saved to: {summary_file}")
        print("=" * 60)

    def run(self) -> None:
        """Run the endpoint output generation process."""
        logger.info("Starting endpoint output generation")
        logger.info(f"Base URL: {self.base_url}")
        logger.info(f"Output directory: {self.output_dir}")

        # Load OpenAPI specification
        self.load_openapi_spec()

        # Extract endpoints
        endpoints = self.extract_endpoints()

        if not endpoints:
            logger.error("No endpoints found in OpenAPI specification")
            return

        # Execute each endpoint
        for i, endpoint in enumerate(endpoints, 1):
            logger.info(f"Processing endpoint {i}/{len(endpoints)}: {endpoint['method']} {endpoint['path']}")
            self.execute_endpoint(endpoint)

        # Generate summary report
        self.generate_summary_report()

        logger.info("Endpoint output generation completed")


def main():
    """Main function to run the endpoint output generator."""
    import argparse

    parser = argparse.ArgumentParser(description='Generate endpoint outputs from OpenAPI specification')
    parser.add_argument('--openapi-file', default='openapi.json',
                        help='Path to OpenAPI specification file (default: openapi.json)')
    parser.add_argument('--base-url',
                        help='Base URL for the API (default: from API_BASE_URL env var or http://localhost:8000)')
    parser.add_argument('--fetch-openapi', action='store_true',
                        help='Force fetch OpenAPI spec from API (default: auto-fetch with file fallback)')
    parser.add_argument('--use-local-only', action='store_true',
                        help='Use only local OpenAPI file, do not fetch from API')
    parser.add_argument('--verbose', '-v', action='store_true',
                        help='Enable verbose logging')

    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    # Create and run the generator
    generator = EndpointOutputGenerator(
        openapi_file=args.openapi_file,
        base_url=args.base_url,
        use_local_only=args.use_local_only
    )

    try:
        generator.run()
    except KeyboardInterrupt:
        logger.info("Process interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()