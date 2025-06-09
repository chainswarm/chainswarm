from typing import Dict, List, Any
from datetime import datetime
from loguru import logger

class BlockDataProcessor:
    """Handles processing of block data into standardized formats."""

    @staticmethod
    def process_traces(block: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Process trace data from a block.
        
        Args:
            block: Raw block data containing traces
            
        Returns:
            List of processed trace records
        """
        traces = []
        for trace in block.get("traces", []):
            if trace and 'data' in trace and 'stringValues' in trace['data']:
                data = trace['data']['stringValues']
                processed_trace = {
                    'block_height': block['block_height'],
                    'timestamp': block['timestamp'],
                    'key': data['key'],
                    'method': data['method'],
                    'value': data.get('value'),
                    'value_encoded': data.get('value_encoded'),
                    'result': data.get('result'),
                    'result_encoded': data.get('result_encoded')
                }
                traces.append(processed_trace)
        return traces

    @staticmethod
    def process_events(block: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Process event data from a block.
        
        Args:
            block: Raw block data containing events
            
        Returns:
            List of processed event records
        """
        events = []
        for event in block.get('events', []):
            try:
                if not hasattr(event, 'value'):
                    continue

                # Handle scale_info type events
                if 'scale_info' in str(event):
                    if hasattr(event, 'type'):
                        module_name = str(event.type).split('::')[0]
                    else:
                        module_name = str(event).split('::')[0]
                else:
                    module_name = str(event.value.get('module_id', ''))

                event_data = {
                    'block_height': block['block_height'],
                    'extrinsic_index': event.value.get('extrinsic_idx'),
                    'timestamp': block['timestamp'],
                    'module': module_name,
                    'event': str(event.value.get('event_id', '')),
                    'attributes': event.value.get('attributes', []),
                    'phase': str(event.value.get('phase', ''))
                }
                events.append(event_data)
                
            except Exception as e:
                logger.error(f"Error processing event: {str(e)} | Raw event: {str(event)}")
                raise
                
        return events

    @staticmethod
    def process_extrinsics(block: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Process extrinsic data from a block.
        
        Args:
            block: Raw block data containing extrinsics
            
        Returns:
            List of processed extrinsic records
        """
        extrinsics = []
        for extrinsic in block.get('extrinsics', []):
            processed_extrinsic = {
                'block_height': block['block_height'],
                'timestamp': block['timestamp'],
                'module': '',
                'call': '',
                'args': [],
                'error': ''
            }
            
            if hasattr(extrinsic, 'call'):
                call = extrinsic.call
                processed_extrinsic.update({
                    'module': call.module.name,
                    'call': call.function.name,
                    'args': [{
                        'name': param.name,
                        'type': param.type,
                        'value': str(param.value)
                    } for param in call.params],
                    'error': str(getattr(extrinsic, 'error', None))
                })
            extrinsics.append(processed_extrinsic)
            
        return extrinsics

    @classmethod
    def process_block(cls, block: Dict[str, Any]) -> Dict[str, Any]:
        """Process all data types from a block.
        
        Args:
            block: Raw block data
            
        Returns:
            Dictionary containing block data with processed traces, events, and extrinsics
        """
        return {
            'block_height': block['block_height'],
            'block_hash': block['block_hash'],
            'timestamp': block['timestamp'],
            'traces': cls.process_traces(block),
            'events': cls.process_events(block),
            'extrinsics': cls.process_extrinsics(block)
        }

    @staticmethod
    def group_items_by_block(items: List[Dict[str, Any]], topic: str = None) -> Dict[int, Dict[str, Any]]:
        """Group items by block height for batch processing.
        
        Args:
            items: List of block items, each containing all data types
            topic: Deprecated parameter, kept for backward compatibility
            
        Returns:
            Dictionary of blocks grouped by block height
        """
        items_by_block = {}
        for item in items:
            block_height = item['block_height']
            
            if block_height not in items_by_block:
                items_by_block[block_height] = {
                    'block_height': block_height,
                    'block_hash': item['block_hash'],
                    'timestamp': item['timestamp'],
                    'traces': item['traces'],
                    'events': item['events'],
                    'extrinsics': item['extrinsics']
                }
                
        return items_by_block