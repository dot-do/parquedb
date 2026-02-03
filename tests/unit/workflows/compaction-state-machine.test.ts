/**
 * State Machine Tests for Compaction Queue Consumer
 *
 * Tests the state machine validation that ensures proper state transitions
 * for compaction window processing.
 */

import { describe, it, expect } from 'vitest'
import {
  WINDOW_STATE_TRANSITIONS,
  TRANSITION_DESCRIPTIONS,
  InvalidStateTransitionError,
  isValidStateTransition,
  validateStateTransition,
  getStateName,
  getTransitionDescription,
  type WindowStateName,
} from '../../../src/workflows/compaction-queue-consumer'

describe('Window Processing State Machine', () => {
  describe('WINDOW_STATE_TRANSITIONS', () => {
    it('should define valid transitions for pending state', () => {
      expect(WINDOW_STATE_TRANSITIONS.pending).toEqual(['processing'])
    })

    it('should define valid transitions for processing state', () => {
      expect(WINDOW_STATE_TRANSITIONS.processing).toContain('dispatched')
      expect(WINDOW_STATE_TRANSITIONS.processing).toContain('pending')
    })

    it('should define valid transitions for dispatched state', () => {
      expect(WINDOW_STATE_TRANSITIONS.dispatched).toContain('deleted')
      expect(WINDOW_STATE_TRANSITIONS.dispatched).toContain('pending')
    })

    it('should define deleted as terminal state with no transitions', () => {
      expect(WINDOW_STATE_TRANSITIONS.deleted).toEqual([])
    })
  })

  describe('TRANSITION_DESCRIPTIONS', () => {
    it('should have descriptions for all valid transitions', () => {
      expect(TRANSITION_DESCRIPTIONS['pending→processing']).toBeDefined()
      expect(TRANSITION_DESCRIPTIONS['processing→dispatched']).toBeDefined()
      expect(TRANSITION_DESCRIPTIONS['processing→pending']).toBeDefined()
      expect(TRANSITION_DESCRIPTIONS['dispatched→deleted']).toBeDefined()
      expect(TRANSITION_DESCRIPTIONS['dispatched→pending']).toBeDefined()
    })
  })

  describe('isValidStateTransition', () => {
    describe('from pending state', () => {
      it('should allow transition to processing', () => {
        expect(isValidStateTransition('pending', 'processing')).toBe(true)
      })

      it('should reject transition to dispatched', () => {
        expect(isValidStateTransition('pending', 'dispatched')).toBe(false)
      })

      it('should reject transition to deleted', () => {
        expect(isValidStateTransition('pending', 'deleted')).toBe(false)
      })

      it('should reject self-transition', () => {
        expect(isValidStateTransition('pending', 'pending')).toBe(false)
      })
    })

    describe('from processing state', () => {
      it('should allow transition to dispatched', () => {
        expect(isValidStateTransition('processing', 'dispatched')).toBe(true)
      })

      it('should allow transition back to pending (rollback)', () => {
        expect(isValidStateTransition('processing', 'pending')).toBe(true)
      })

      it('should reject transition to deleted', () => {
        expect(isValidStateTransition('processing', 'deleted')).toBe(false)
      })

      it('should reject self-transition', () => {
        expect(isValidStateTransition('processing', 'processing')).toBe(false)
      })
    })

    describe('from dispatched state', () => {
      it('should allow transition to deleted (success)', () => {
        expect(isValidStateTransition('dispatched', 'deleted')).toBe(true)
      })

      it('should allow transition back to pending (failure retry)', () => {
        expect(isValidStateTransition('dispatched', 'pending')).toBe(true)
      })

      it('should reject transition to processing', () => {
        expect(isValidStateTransition('dispatched', 'processing')).toBe(false)
      })

      it('should reject self-transition', () => {
        expect(isValidStateTransition('dispatched', 'dispatched')).toBe(false)
      })
    })

    describe('from deleted state', () => {
      it('should reject all transitions (terminal state)', () => {
        expect(isValidStateTransition('deleted', 'pending')).toBe(false)
        expect(isValidStateTransition('deleted', 'processing')).toBe(false)
        expect(isValidStateTransition('deleted', 'dispatched')).toBe(false)
        expect(isValidStateTransition('deleted', 'deleted')).toBe(false)
      })
    })
  })

  describe('validateStateTransition', () => {
    it('should not throw for valid transitions', () => {
      expect(() => validateStateTransition('pending', 'processing', 'window-1')).not.toThrow()
      expect(() => validateStateTransition('processing', 'dispatched', 'window-1')).not.toThrow()
      expect(() => validateStateTransition('processing', 'pending', 'window-1')).not.toThrow()
      expect(() => validateStateTransition('dispatched', 'deleted', 'window-1')).not.toThrow()
      expect(() => validateStateTransition('dispatched', 'pending', 'window-1')).not.toThrow()
    })

    it('should throw InvalidStateTransitionError for invalid transitions', () => {
      expect(() => validateStateTransition('pending', 'dispatched', 'window-1')).toThrow(InvalidStateTransitionError)
      expect(() => validateStateTransition('processing', 'deleted', 'window-1')).toThrow(InvalidStateTransitionError)
      expect(() => validateStateTransition('dispatched', 'processing', 'window-1')).toThrow(InvalidStateTransitionError)
      expect(() => validateStateTransition('deleted', 'pending', 'window-1')).toThrow(InvalidStateTransitionError)
    })

    it('should include window key in error message', () => {
      try {
        validateStateTransition('pending', 'dispatched', 'test-window-123')
        expect.fail('Should have thrown')
      } catch (e) {
        expect(e).toBeInstanceOf(InvalidStateTransitionError)
        const error = e as InvalidStateTransitionError
        expect(error.message).toContain('test-window-123')
        expect(error.windowKey).toBe('test-window-123')
      }
    })

    it('should include from and to states in error', () => {
      try {
        validateStateTransition('pending', 'dispatched', 'window-1')
        expect.fail('Should have thrown')
      } catch (e) {
        expect(e).toBeInstanceOf(InvalidStateTransitionError)
        const error = e as InvalidStateTransitionError
        expect(error.fromState).toBe('pending')
        expect(error.toState).toBe('dispatched')
      }
    })

    it('should include valid transitions in error message', () => {
      try {
        validateStateTransition('pending', 'dispatched', 'window-1')
        expect.fail('Should have thrown')
      } catch (e) {
        expect(e).toBeInstanceOf(InvalidStateTransitionError)
        const error = e as InvalidStateTransitionError
        expect(error.message).toContain('processing')
      }
    })

    it('should include reason in error message when provided', () => {
      try {
        validateStateTransition('pending', 'dispatched', 'window-1', 'Workflow already completed')
        expect.fail('Should have thrown')
      } catch (e) {
        expect(e).toBeInstanceOf(InvalidStateTransitionError)
        const error = e as InvalidStateTransitionError
        expect(error.message).toContain('Workflow already completed')
        expect(error.reason).toBe('Workflow already completed')
      }
    })
  })

  describe('getStateName', () => {
    it('should return state name from pending status', () => {
      expect(getStateName({ state: 'pending' })).toBe('pending')
    })

    it('should return state name from processing status', () => {
      expect(getStateName({ state: 'processing', startedAt: Date.now() })).toBe('processing')
    })

    it('should return state name from dispatched status', () => {
      expect(getStateName({
        state: 'dispatched',
        workflowId: 'wf-123',
        dispatchedAt: Date.now()
      })).toBe('dispatched')
    })
  })

  describe('getTransitionDescription', () => {
    it('should return description for valid transitions', () => {
      expect(getTransitionDescription('pending', 'processing')).toBe(
        'Window ready for compaction, starting workflow dispatch'
      )
      expect(getTransitionDescription('processing', 'dispatched')).toBe(
        'Workflow successfully created, awaiting completion'
      )
      expect(getTransitionDescription('processing', 'pending')).toBe(
        'Workflow dispatch failed or timed out, will retry'
      )
      expect(getTransitionDescription('dispatched', 'deleted')).toBe(
        'Workflow completed successfully, window cleaned up'
      )
      expect(getTransitionDescription('dispatched', 'pending')).toBe(
        'Workflow failed, resetting for retry'
      )
    })

    it('should return generic description for invalid/unknown transitions', () => {
      expect(getTransitionDescription('pending', 'dispatched')).toBe('pending → dispatched')
      expect(getTransitionDescription('deleted', 'pending')).toBe('deleted → pending')
    })
  })

  describe('InvalidStateTransitionError', () => {
    it('should have correct name', () => {
      const error = new InvalidStateTransitionError('pending', 'dispatched', 'window-1')
      expect(error.name).toBe('InvalidStateTransitionError')
    })

    it('should be an instance of Error', () => {
      const error = new InvalidStateTransitionError('pending', 'dispatched', 'window-1')
      expect(error).toBeInstanceOf(Error)
    })

    it('should expose all properties', () => {
      const error = new InvalidStateTransitionError('pending', 'dispatched', 'window-1', 'test reason')
      expect(error.fromState).toBe('pending')
      expect(error.toState).toBe('dispatched')
      expect(error.windowKey).toBe('window-1')
      expect(error.reason).toBe('test reason')
    })
  })

  describe('State Transition Graph Coverage', () => {
    // These tests verify the complete state transition graph

    it('should cover the happy path: pending -> processing -> dispatched -> deleted', () => {
      const happyPath: [WindowStateName, WindowStateName][] = [
        ['pending', 'processing'],
        ['processing', 'dispatched'],
        ['dispatched', 'deleted'],
      ]

      for (const [from, to] of happyPath) {
        expect(isValidStateTransition(from, to)).toBe(true)
      }
    })

    it('should cover rollback path: pending -> processing -> pending', () => {
      expect(isValidStateTransition('pending', 'processing')).toBe(true)
      expect(isValidStateTransition('processing', 'pending')).toBe(true)
    })

    it('should cover failure retry path: pending -> processing -> dispatched -> pending', () => {
      expect(isValidStateTransition('pending', 'processing')).toBe(true)
      expect(isValidStateTransition('processing', 'dispatched')).toBe(true)
      expect(isValidStateTransition('dispatched', 'pending')).toBe(true)
    })

    it('should verify no backward transitions from deleted', () => {
      // deleted is a terminal state
      const allStates: WindowStateName[] = ['pending', 'processing', 'dispatched', 'deleted']
      for (const state of allStates) {
        expect(isValidStateTransition('deleted', state)).toBe(false)
      }
    })

    it('should verify no direct skip from pending to dispatched', () => {
      expect(isValidStateTransition('pending', 'dispatched')).toBe(false)
    })

    it('should verify no direct skip from pending to deleted', () => {
      expect(isValidStateTransition('pending', 'deleted')).toBe(false)
    })
  })
})
